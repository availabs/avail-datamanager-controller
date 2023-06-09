import { stateAbbr2FipsCode } from "../../src/data_utils/constants/stateFipsCodes";

export const PG_ENV = process.env.AVAIL_PG_ENV || "dama_dev_1";

export type TableInfo = {
  parent_table_schema: string | null;
  parent_table_name: string | null;
  table_schema: string;
  table_name: string;
  depth: number;
};

export type InheritanceTree = TableInfo[];

export const state_re = new RegExp(
  `^${Object.keys(stateAbbr2FipsCode)
    .map((s) => `(${s})`)
    .join("|")}$`
);
