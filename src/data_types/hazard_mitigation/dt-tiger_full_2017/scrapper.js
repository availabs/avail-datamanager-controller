const axios = require('axios');
const cheerio = require('cheerio')

// const url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/';

const getFiles = async (url) => {
    let files = []
    await axios(url)
        .then(response => {
            const html = response.data;
            const $ = cheerio.load(html);
            const statsTable = $('tbody').children();

            statsTable.each(function () {
                let row =  $(this).find('td').text();
                // console.log(row.split('.zip')[0])
                row.includes('.zip') && files.push(`${row.split('.zip')[0]}.zip`)
            })
            return files;
        });
    return files;
}

module.exports = {
    getFiles
}
