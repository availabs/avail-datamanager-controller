/*
  The analyzeSchema function consumes an object AsyncGenerator
  and determines the PostgreSQL table column types for the
  respective object fields.
*/

import _ from "lodash";

const SAMPLES_LEN = 10;

const decimalPointRE = /\./;

// NOTE: 0 and 1 handled by converting to JS number to allow
//       transitioning from BOOLEAN to pgNumericType
const booleanTextRE =
  /(^t$)|(^f$)|(^y$)|(^n$)|(^true$)|(^false$)|(^yes$)|(^no$)/i;

export enum PgDataType {
  BOOLEAN = "BOOLEAN",
  SMALLINT = "SMALLINT",
  INT = "INTEGER",
  BIGINT = "BIGINT",
  REAL = "REAL",
  DOUBLE = "DOUBLE PRECISION",
  NUMERIC = "NUMERIC",
  DATE = "DATE",
  TIMESTAMP = "TIMESTAMP",
  TEXT = "TEXT",
}

export const pgIntegerTypes = [
  PgDataType.SMALLINT,
  PgDataType.INT,
  PgDataType.BIGINT,
];
export const pgDecimalTypes = [
  PgDataType.REAL,
  PgDataType.DOUBLE,
  PgDataType.NUMERIC,
];
export const pgNumericTypes = [...pgIntegerTypes, ...pgDecimalTypes];
export const pgDateTypes = [PgDataType.DATE, PgDataType.TIMESTAMP];

type JsType = "number" | "string" | "null";

type SchemaFieldSummary = {
  null: number;
  nonnull: number;
  types: Record<
    JsType,
    {
      count: number;
      samples: [number | string];
    }
  >;
  db_type: PgDataType;
};

type SchemaFieldAnalysis = {
  key: string;
  summary: SchemaFieldSummary;
};

export type SchemaAnalysis = SchemaFieldAnalysis[];

export default async function analyzeSchema(
  objIter: AsyncGenerator<object>,
  schemaAnalysis: SchemaAnalysis = []
): Promise<{ objectsCount: number; schemaAnalysis: SchemaAnalysis }> {
  let i = 0;
  let objectsCount = 0;

  const keyIdxs = schemaAnalysis.reduce(
    (acc: Record<string, number>, { key }, i) => {
      acc[key] = i;
      return acc;
    },
    {}
  );

  for await (const d of objIter) {
    // console.log(JSON.stringify(d, null, 4));
    try {
      ++objectsCount;

      const hadTextualBoolean = new Set();

      for (const k of Object.keys(d)) {
        keyIdxs[k] = Number.isFinite(keyIdxs[k]) ? keyIdxs[k] : i++;

        schemaAnalysis[keyIdxs[k]] = <SchemaFieldAnalysis>(
          schemaAnalysis[keyIdxs[k]]
        ) || {
          key: k,
          summary: {
            null: 0,
            nonnull: 0,
            types: {},
            db_type: null,
          },
        };

        const { summary } = schemaAnalysis[keyIdxs[k]];

        let v: string | number | null;

        if (d[k] instanceof Date) {
          v = (<Date>d[k]).toISOString();
        } else if (typeof d[k] === "number") {
          v = <number>d[k];
        } else if (d[k] === null || d[k] === undefined) {
          v = null;
        } else {
          v = `${d[k]}`;
        }

        let t = v === null ? null : typeof v;

        if (t === "string") {
          // @ts-ignore
          v = v.trim();
          if (v === "") {
            t = null;
          }
        }

        pgTypeCheck: if (
          t !== null && // null tells us nothing
          v !== "" && // empty string tells us nothing
          summary.db_type !== PgDataType.TEXT // Once TEXT, always TEXT
        ) {
          // Hard to stay BOOLEAN
          pgBooleanTypeCheck: if (
            summary.db_type === null ||
            summary.db_type === PgDataType.BOOLEAN
          ) {
            if (t === "boolean") {
              summary.db_type = PgDataType.BOOLEAN;
              break pgTypeCheck;
            }
            // NOTE: We handle numeric values differently than strings
            //       to support transitioning from BOOLEAN to pgNumericType
            // @ts-ignore see pgTypeCheck IF condition
            const n = +v;

            if (Number.isFinite(n)) {
              if (n === 0 || n === 1) {
                summary.db_type = PgDataType.BOOLEAN;
                break pgTypeCheck;
              }

              // v is a number out of BOOLEAN range [0,1]

              if (hadTextualBoolean.has(k)) {
                // Had non-pgDateType text, now numeric out of boolean range => TEXT
                summary.db_type = PgDataType.TEXT;
                break pgTypeCheck;
              }

              summary.db_type = PgDataType.SMALLINT;
              break pgBooleanTypeCheck;
              // falls through in pgTypeCheck. Will enter pgNumericTypeCheck below.
            }

            if (t === "string") {
              // @ts-ignore
              if (booleanTextRE.test(v.trim())) {
                hadTextualBoolean.add(k); // Can no longer be an pgIntegerType
                summary.db_type = PgDataType.BOOLEAN;
                break pgTypeCheck;
              } else if (hadTextualBoolean.has(k)) {
                // cannot be a pgDateTypes
                summary.db_type = PgDataType.TEXT;
                break pgTypeCheck;
              } else if (summary.db_type === null) {
                // could be pgDateType
                break pgBooleanTypeCheck;
                // falls through in pgTypeCheck
              }
            }

            summary.db_type = PgDataType.TEXT;
          }

          // Check if property maps to pgDateTypes
          pgDateTypeCheck: if (
            summary.db_type === null ||
            pgDateTypes.includes(summary.db_type)
          ) {
            // @ts-ignore see pgTypeCheck IF condition
            if (Number.isFinite(+v)) {
              if (summary.db_type === null) {
                // May still be pgNumericType
                break pgDateTypeCheck;
              } else {
                // console.error(k, "!Number.isFinite", v);
                // pgDateTypes eliminated
                summary.db_type = PgDataType.TEXT;
                break pgTypeCheck;
              }
            }

            // @ts-ignore see pgTypeCheck IF condition
            const date = new Date(v);
            let isValidDate = Number.isFinite(date.getTime());

            // Because new Date('Foo 2021') yields 2021-01-01T05:00:00.000Z
            // If the string starts with non-numeric characters
            if (isValidDate && /^[^0-9+-]/.test(`${v}`)) {
              const d1 = date.toISOString();
              const d2 = new Date(
                `${v}`.trim().replace(/^[a-z]/gi, "")
              ).toISOString();

              // And those non-numeric starting characters are ignored.
              if (d1 === d2) {
                // And those characters don't represent "January"
                if (!/^jan(|u|ua|uar|uary) /i.test(`${v}`.trim())) {
                  // It's an invalid date.
                  isValidDate = false;
                }
              }
            }

            if (!isValidDate) {
              // console.error(k, "!isValidDate", v);
              summary.db_type = PgDataType.TEXT;
              break pgTypeCheck;
            }

            if (summary.db_type === null) {
              summary.db_type = PgDataType.DATE;
              // falls through
            }

            // @ts-ignore
            if (summary.db_type === PgDataType.DATE && /T|:/.test(v)) {
              summary.db_type = PgDataType.TIMESTAMP;
            }

            break pgTypeCheck;
          }

          // Check if pgNumericType
          //   See: https://www.postgresql.org/docs/11/datatype-numeric.html
          /* pgNumericTypeCheck : */ if (
            summary.db_type === null ||
            pgNumericTypes.includes(summary.db_type)
          ) {
            // @ts-ignore see pgTypeCheck IF condition
            const n = +v;
            const s = `${v}`; // v = "1.0", `${v}` = "1.0"

            // NOTE: Assumes we already eliminated pgDateTypes above.
            if (!Number.isFinite(n)) {
              summary.db_type = PgDataType.TEXT;
              break pgTypeCheck;
            }

            // The value parses to a valid number.

            // NUMERIC is the sink for pgNumericTypes
            if (summary.db_type === PgDataType.NUMERIC) {
              break pgTypeCheck;
            }

            const containsDecimalPoint = decimalPointRE.test(s);

            // Once a pgDecimalType, cannot be a pgIntegerType
            if (containsDecimalPoint) {
              // First we convert current pgIntegerType to the corresponding pgDecimalType
              pgInt2Decimal: if (pgIntegerTypes.includes(summary.db_type)) {
                if (summary.db_type === PgDataType.SMALLINT) {
                  summary.db_type = PgDataType.REAL;
                  break pgInt2Decimal;
                  // falls through in if(containsDecimalPoint)
                }

                if (summary.db_type === PgDataType.INT) {
                  summary.db_type = PgDataType.DOUBLE;
                  break pgInt2Decimal;
                  // falls through in if(containsDecimalPoint)
                }

                if (summary.db_type === PgDataType.BIGINT) {
                  // We've reached the sink for pgNumericTypes
                  summary.db_type = PgDataType.NUMERIC;
                  break pgTypeCheck;
                }
              }

              if (summary.db_type === null) {
                summary.db_type = PgDataType.REAL;
                // falls through
              }

              // REAL supports 6 decimal digits of precision
              if (summary.db_type === PgDataType.REAL && s.length > 7) {
                summary.db_type = PgDataType.DOUBLE;
                // falls through
              }

              // DOUBLE PRECISION supports 15 decimal digits of precision
              if (summary.db_type === PgDataType.DOUBLE && s.length > 16) {
                summary.db_type = PgDataType.NUMERIC;
                // falls through
              }

              break pgTypeCheck;
            }

            // n does not contain a decimal point

            if (
              summary.db_type === PgDataType.REAL ||
              summary.db_type === PgDataType.DOUBLE
            ) {
              // We already assigned a pgDecimalType for this property.
              // Now we check if the current integer exceeds the pgDecimalType range.

              if (summary.db_type === PgDataType.REAL && s.length > 6) {
                summary.db_type = PgDataType.DOUBLE;
                // falls through
              }
              if (summary.db_type === PgDataType.DOUBLE && s.length > 15) {
                // upgrade DOUBLE to NUMERIC
                summary.db_type = PgDataType.NUMERIC;
                // falls through
              }

              break pgTypeCheck;
            }

            // We either have not
            if (summary.db_type === null) {
              summary.db_type = PgDataType.SMALLINT;
              // falls through
            }

            if (
              summary.db_type === PgDataType.SMALLINT &&
              Math.abs(n) > 32767
            ) {
              summary.db_type = PgDataType.INT;
              // falls through
            }

            if (
              summary.db_type === PgDataType.INT &&
              Math.abs(n) > 2147483648
            ) {
              summary.db_type = PgDataType.BIGINT;
              // falls through
            }

            break pgTypeCheck;
          }

          // Default Case
          summary.db_type = PgDataType.TEXT;
        }

        if (t === "string") {
          // @ts-ignore
          v = v.slice(0, 32);
        }

        if (t !== null) {
          ++summary.nonnull;

          // collect a sample of values
          const typeSummary = (summary.types[t] = summary.types[t] || {
            count: 0,
            samples: [],
          });

          ++typeSummary.count;

          if (
            t === "string" &&
            // @ts-ignore
            /,/.test(v) &&
            !typeSummary.samples.includes(v)
          ) {
            // Prefer the strings with the most commas in case it should be a PostgreSQL ARRAY
            typeSummary.samples.push(v);

            typeSummary.samples.sort(
              (a: any, b: any) =>
                `${b}`.replace(/[^,]/g, "").length -
                `${a}`.replace(/[^,]/g, "").length
            );
            typeSummary.samples.length = Math.min(
              typeSummary.samples.length,
              SAMPLES_LEN
            );
          } else if (
            pgNumericTypes.includes(summary.db_type) &&
            !typeSummary.samples.includes(v)
          ) {
            // Prefer the largest numeric values for deciding numeric column type
            typeSummary.samples.push(v);

            typeSummary.samples.sort((a: any, b: any) => +b - +a);
            typeSummary.samples.length = Math.min(
              typeSummary.samples.length,
              SAMPLES_LEN
            );
          } else if (
            typeSummary.samples.length < SAMPLES_LEN &&
            !typeSummary.samples.includes(v)
          ) {
            typeSummary.samples.push(v);
          }
        } else {
          ++summary.null;
        }
      }
    } catch (err) {
      console.error(err);
    }
  }

  return { objectsCount, schemaAnalysis };
}
