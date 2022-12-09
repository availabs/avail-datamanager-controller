const  fs = require('fs');
const _ = require('lodash');
var sql = require('sql');
const {tables} = require('./tables')

sql.setDialect('postgres');

const crop_loss = sql.define(tables.usda_crop_insurance_cause_of_loss)
console.log(crop_loss.create().ifNotExists().toQuery())

const converToBool = (value) =>
    value === '0.0' ? false :
    value === '1.0' ? true : value

const formatValue = (value) => value.trim().replace(/\0/g, ``)

export const loadFiles = async (ctx, table = 'usda_crop_insurance_cause_of_loss') => {
    console.warn('for bigger files:  node --max-old-space-size=8192 upload.js ')
    console.log('Creating Table', table)

    await ctx.call("dama_db.query", {
      text: crop_loss.create().ifNotExists().toQuery().text,
    });

    console.log('uploading')

    let dataFolder = './data/usda/'
    let files = fs.readdirSync(dataFolder).filter(f => f.substr(0, 1) !== '.' && !f.includes('.zip')); // filtering any open files

    return files
        .reduce(async (acc, file, fileI) => {
          console.log('file', file)
            await acc;
            return new Promise((resolve, reject) => {

                console.log(`file ${++fileI} of ${files.length} ${fileI*100/files.length}% ${file}`)
                fs.readFile(dataFolder + file, 'utf8', (err,d) => {

                    let headers = [
                            'commodity_year_identifier', 'state_fips', 'state_abbr', 'county_fips', 'county_name', 'commodity_code', 'commodity_name',
                            'insurance_plan_code', 'insurance_plan_name_abbr', 'coverage_category', 'stage_code', 'cause_of_loss_code', 'cause_of_loss_desc',
                            'month_of_loss', 'month_of_loss_name', 'year_of_loss',

                            'policies_earning_premium', 'policies_indemnified', 'net_planted_acres', 'net_endorsed_acres', 'liability', 'total_premium', 'producer_paid_premium',
                            'subsidy', 'state_or_private_subsidy', 'additional_subsidy', 'efa_premium_discount', 'net_determined_acres', 'indemnity_amount',
                            'loss_ratio'
                        ]

                    let boolCols = tables[table].booleanColumns
                    let numCols = tables[table].numericColumns
                    let dateCols = tables[table].dateColumns
                    let floatCols = tables[table].floatColumns

                    let lines = d.split(/\r?\n/)

                    resolve(
                        _.chunk(lines, 1500)
                            .reduce(async (accLines, currLines, linesIndex) => {

                                // if (linesIndex < 2 ) return Promise.resolve();

                                await accLines;
                                console.log(linesIndex, `${linesIndex*1500} / ${lines.length}`, (linesIndex*1500*100)/lines.length)

                                let values =
                                    currLines
                                        .map(d1 => d1.split('|'))
                                        .filter(d2 => d2.length > 1)
                                        .map((d2) => {
                                            return d2.reduce((acc, value, index) => {
                                                acc[headers[index]] =
                                                    (numCols || []).includes(headers[index]) && [null, '', ' ', undefined].includes(value) ?
                                                        0 :
                                                        (floatCols || []).includes(headers[index]) && [null, '', ' ', undefined].includes(value) ?
                                                        0.0 :
                                                        (dateCols || []).includes(headers[index]) && [0, '0'].includes(value) ?
                                                            null :
                                                            (numCols || []).includes(headers[index]) && typeof value !== "number" && value ?
                                                                parseInt(value) :
                                                                (floatCols || []).includes(headers[index]) && typeof value !== "number" && value ?
                                                                    parseFloat(value) :
                                                                    (boolCols || []).includes(headers[index]) ? converToBool(value) :
                                                                    formatValue(value)
                                                return acc;
                                            }, {})
                                        });

                                let query = crop_loss.insert(values).toQuery();

                                return ctx.call("dama_db.query", query);
                            }, Promise.resolve())
                    )
                })
            })
    }, Promise.resolve())

}
