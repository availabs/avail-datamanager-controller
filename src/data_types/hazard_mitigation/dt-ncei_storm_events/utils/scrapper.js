const axios = require('axios');
const cheerio = require('cheerio')

const url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/';

const getFiles = async () => {
    let files = {details: [], fatalities: [], locations: []}
    await axios(url)
        .then(response => {
            const html = response.data;
            const $ = cheerio.load(html);
            const statsTable = $('tbody').children();

            statsTable.each(function () {
                let row =  $(this).find('td').text();
                if(row.includes('StormEvents')){
                    let tmpStr = row.split('StormEvents_')[1].split('-');
                    let table = tmpStr[0];
                    // let year = tmpStr[1].split('_d')[1].split('_')[0];
                    files[table].push(`${row.split('.gz')[0]}.gz`)
                }
            })
            return files
        });
    return files;
}

module.exports = {
    getFiles
}