const fs = require("fs");
const _ = require("lodash");
const sql = require("sql");
const {tables} = require("./tables");

sql.setDialect("postgres");

const converToBool = (value) =>
  value === "0.0" ? false :
    value === "1.0" ? true : value;

const formatValue = (value) => value.trim().replace(/\0/g, "");

export const loadFiles = async (ctx, view_id, table = "nri") => {
  console.warn("for bigger files:  node --max-old-space-size=8192 upload.js ");
  console.log("Creating Table", table);

  const nri = sql.define(tables[table](view_id));
  await ctx.call("dama_db.query", {text: nri.create().ifNotExists().toQuery().text});

  console.log("uploading");

  const dataFolder = `./data/${table}/`;
  console.log('files', fs.readdirSync(dataFolder))
  const files = fs.readdirSync(dataFolder).filter(f => f === "NRI_Table_Counties.csv"); // filtering any open files

  return files
    .reduce(async (acc, file, fileI) => {
      await acc;
      return new Promise((resolve, reject) => {

        console.log(`file ${++fileI} of ${files.length} ${fileI * 100 / files.length}% ${file}`);
        fs.readFile(dataFolder + file, "utf8", (err, d) => {

          const headers = d.split(/\r?\n/).slice(0, 1)[0].split(",");

          const boolCols = tables[table](view_id).booleanColumns;
          const numCols = tables[table](view_id).numericColumns;
          const dateCols = tables[table](view_id).dateColumns;
          const floatCols = tables[table](view_id).floatColumns;
          // console.log('headers', headers)

          const lines = d.split(/\r?\n/).slice(1, d.split(/\r?\n/).length);

          resolve(
            _.chunk(lines, 50)
              .reduce(async (accLines, currLines, linesIndex) => {

                // if (linesIndex < 2 ) return Promise.resolve();

                await accLines;
                console.log(linesIndex, `${linesIndex * 1500} / ${lines.length}`, (linesIndex * 1500 * 100) / lines.length);

                const values =
                  currLines
                    .map(d1 => d1.split(","))
                    .filter(d2 => d2.length > 1)
                    .map((d2) => {
                      return d2.reduce((acc, value, index) => {
                        console.log(headers[index])
                        if (tables[table](view_id).columns.map(c => c.name).includes(headers[index].toLowerCase())) {
                          acc[headers[index].toLowerCase()] =
                            (numCols || []).includes(headers[index]) && [null, "", " ", undefined].includes(value) ?
                              0 :
                              (floatCols || []).includes(headers[index]) && [null, "", " ", undefined].includes(value) ?
                                0.0 :
                                (dateCols || []).includes(headers[index]) && [0, "0"].includes(value) ?
                                  null :
                                  (numCols || []).includes(headers[index]) && typeof value !== "number" && value ?
                                    parseInt(value) :
                                    (floatCols || []).includes(headers[index]) && typeof value !== "number" && value ?
                                      parseFloat(value) :
                                      (boolCols || []).includes(headers[index]) ? converToBool(value) :
                                        formatValue(value);
                        }
                        return acc;
                      }, {});
                    })
                    .filter(d2 => d2);

                const query = nri.insert(values).toQuery();
                console.log(query)
                return ctx.call("dama_db.query", query);
              }, Promise.resolve())
          );
        });
      });
    }, Promise.resolve());

};
