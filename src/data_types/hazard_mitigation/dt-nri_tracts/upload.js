var csv = require('csv-parser');
const fs = require("fs");
const _ = require("lodash");
const sql = require("sql");
const {tables} = require("./tables");

sql.setDialect("postgres");

const converToBool = (value) =>
  value === "0.0" ? false :
    value === "1.0" ? true : value;

const formatValue = (value) => value?.trim()?.replace(/\0/g, "");

export const loadFiles = async (ctx, view_id, table = "nri_tracts") => {
  console.warn("for bigger files:  node --max-old-space-size=8192 upload.js ");
  console.log("Creating Table", table);

  const nri = sql.define(tables[table](view_id));
  await ctx.call("dama_db.query", {text: nri.create().ifNotExists().toQuery().text});

  console.log("uploading");

  const dataFolder = `./tmp-etl/${table}/`;
  console.log('files', fs.readdirSync(dataFolder))
  const file = 'NRI_Table_CensusTracts.csv';
  console.log('file:', dataFolder + file);

  return new Promise((resolve, reject) => {

    const promises = [];

    const stream = fs.createReadStream(dataFolder + file, 'utf8').pipe(csv());

    stream.on('error', (err) => reject('<NRITracts> Error reading file. ' + err));

    stream.on('data', async chunk => {
      const headers = Object.keys(chunk);
      const boolCols = tables[table](view_id).booleanColumns;
      const numCols = tables[table](view_id).numericColumns;
      const dateCols = tables[table](view_id).dateColumns;
      const floatCols = tables[table](view_id).floatColumns;

      const values =
        headers.reduce((acc, key) => {
          const value = chunk[key];

          if (tables[table](view_id).columns.map(c => c.name).includes(key.toLowerCase())) {
            acc[key.toLowerCase()] =
              (numCols || []).includes(key) && [null, "", " ", undefined].includes(value) ? 0 :
                (floatCols || []).includes(key) && [null, "", " ", undefined].includes(value) ? 0.0 :
                  (dateCols || []).includes(key) && [0, "0"].includes(value) ? null :
                    (numCols || []).includes(key) && typeof value !== "number" && value ? parseInt(value) :
                      (floatCols || []).includes(key) && typeof value !== "number" && value ? parseFloat(value) :
                        (boolCols || []).includes(key) ? converToBool(value) : formatValue(value);
          }
          return acc;
        }, {})
      console.log(headers)
      const query = nri.insert(values).toQuery();
      promises.push(ctx.call("dama_db.query", query));
    });

    stream.on('end', () => resolve(Promise.all(promises)));
  })
};
