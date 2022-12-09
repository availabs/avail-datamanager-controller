// const axios = require('axios');
const cheerio = require('cheerio')
// const puppeteer = require('puppeteer-core');
// const url = 'https://www.sba.gov/document/report-sba-disaster-loan-data';

const getFiles = async (url = `https://www.sba.gov`) => {
  let files = []
  //
  // const browser = await puppeteer.launch({executablePath: 'path/to/your/chrome.exe'});
  // const page = await browser.newPage();
  // await page.goto(url)
  // await page.evaluate(() => {
  //   const li1 = document.querySelectorAll("#main-content > div > div > div > div > ul");
  //   const li2 = document.querySelectorAll("li");
  //
  //   console.log('1', li1);
  //   console.log('2', li2)
  // });
  //
  // await browser.close();
  const html = `
          <ul><li><strong>Version Fiscal Year 2021</strong><strong>|</strong>Effective: 2022-02-11.<a href="/sites/default/files/2022-07/SBA_Disaster_Loan_Data_FY21.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2020</strong><strong>|</strong>Effective: 2021-03-15.<a href="/sites/default/files/2022-07/SBA_Disaster_Loan_Data_FY20.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2019</strong><strong>|</strong>Effective: 2020-04-10.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY19.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2018</strong><strong>|</strong>Effective: 2019-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY18.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2017</strong><strong>|</strong>Effective: 2018-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY17_Update_033118.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2016</strong><strong>|</strong>Effective: 2017-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY16.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2015</strong><strong>|</strong>Effective: 2016-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY15.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2014</strong><strong>|</strong>Effective: 2015-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY14.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2013</strong><strong>|</strong>Effective: 2014-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY13.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Superstorm Sandy</strong><strong>|</strong>Effective: 2014-09-23.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_Superstorm_Sandy.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2012</strong><strong>|</strong>Effective: 2013-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY12.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2011</strong><strong>|</strong>Effective: 2012-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY11.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2010</strong><strong>|</strong>Effective: 2011-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY10.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2009</strong><strong>|</strong>Effective: 2010-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY09.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2008</strong><strong>|</strong>Effective: 2009-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY08.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2007</strong><strong>|</strong>Effective: 2008-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY07.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2006</strong><strong>|</strong>Effective: 2007-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY06.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2005</strong><strong>|</strong>Effective: 2006-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY05.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2004</strong><strong>|</strong>Effective: 2005-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY04.xlsx" target="_blank">Download xlsx</a></li><li><strong>Version Fiscal Year 2003</strong><strong>|</strong>Effective: 2004-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY03.xls" target="_blank">Download xls</a></li><li><strong>Version Fiscal Year 2002</strong><strong>|</strong>Effective: 2003-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY02.xls" target="_blank">Download xls</a></li><li><strong>Version Fiscal Year 2001</strong><strong>|</strong>Effective: 2002-10-01.<a href="/sites/default/files/2020-06/SBA_Disaster_Loan_Data_FY01.xls" target="_blank">Download xls</a></li><li><strong>Version Fiscal Year 2000</strong><strong>|</strong>Effective: 2001-10-01.<a href="/sites/default/files/2021-05/SBA_Disaster_Loan_Data_FY00.xlsx" target="_blank">Download xlsx</a></li></ul>
`;
  const $ = cheerio.load(html);

  const statsTable = $('li');

  statsTable.each(function (e, element) {
    let row = $(element).find('li>a').attr().href;
    $(element).text().toLowerCase().includes('year') && files.push(url + row)
  })
  return files;
}

module.exports = {
    getFiles
}
