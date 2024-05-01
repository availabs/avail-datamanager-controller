#!/usr/bin/env node

// This script is included in the repo as an example of how to debug UI Changes to the MDD.

require("ts-node").register();
require("tsconfig-paths").register();

const {
  runInDamaContext
} = require('../../../../../data_manager/contexts/index.ts');

const {
 createNpmrdsDataRangeDownloadRequest,
  default: MDD
} = require("./index.ts") ;

const ElementPaths = require('./ElementPaths.ts');

const etl_context = {
  meta: {
    pgEnv: 'dama_dev_1',
    etl_context_id: -1
  }
}

async function main() {
  const req = createNpmrdsDataRangeDownloadRequest(
    'vt',
    '2024-01-01',
    '2024-01-07',
    true
  );

  console.log(req)

  await runInDamaContext(etl_context, async () => {
    try {
      const mdd = new MDD()
      await mdd.fillOutFormForNpmrdsExportRequest(req)
      mdd._disconnectPage()
    } catch (err) {
      console.error(err)
    }
  })
}

async function main2() {
  await runInDamaContext(etl_context, async () => {
    const mdd = new MDD()
    const page = await mdd._getPage();

    // const sel = "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(1) > li > span.CheckBoxGroup-node-content-wrapper.CheckBoxGroup-node-content-wrapper-open.CheckBoxGroup-node-selected"
    const sel = "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(1) > li"
    await page.click(sel);
  })
}

async function main3() {
  await runInDamaContext(etl_context, async () => {
    const mdd = new MDD()
    const page = await mdd._getPage();

    let els
    for (let i = 0; i < 2; ++i) {
      console.log('i=', i)
      const sel = "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(1) > li"
      await page.click(sel);

      els = await mdd.$$(
        "body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(1) > li > ul > li > span.CheckBoxGroup-checkbox",
        false
      );

      if (els.length === 6) {
        break
      }
    }

    await new Promise(resolve => setTimeout(resolve, 3000)) ;


    let allChecked = true
    console.log('els.length:', els.length)

    for (let i = 0; i < 5; ++i) {
      const el = els[i]
      let classNames = await (await el.getProperty('className')).jsonValue();
      if (!/checked$/.test(classNames)) {
        await page.click(`body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(1) > li > ul > li:nth-child(${i+1}) > span.CheckBoxGroup-node-content-wrapper.CheckBoxGroup-node-content-wrapper-normal`);

        classNames = await (await el.getProperty('className')).jsonValue();

        if (!/checked$/.test(classNames)) {
          throw new Error('Unable to select datasource')
        }
      }
    }

    const aadtEl = els[5]

    let aadtElClassNames = await (await aadtEl.getProperty('className')).jsonValue();

    if (/checked$/.test(aadtElClassNames)) {
      await page.click(`body > main > div > div.QueryFormContainer > div > div.left-panel > form > ol > li:nth-child(6) > div.DatasourceFieldsSelector > ul:nth-child(1) > li > ul > li:nth-child(6) > span.CheckBoxGroup-node-content-wrapper.CheckBoxGroup-node-content-wrapper-normal`);

      aadtElClassNames = await (await aadtEl.getProperty('className')).jsonValue();

      if (/checked$/.test(aadtElClassNames)) {
        throw new Error('Unable to deselect AADT column')
      }
    }
  })
}

async function main4() {
  await runInDamaContext(etl_context, async () => {
    try {
      const mdd = new MDD()
      await mdd.selectCompletePassengerVehicleMeasures()
      await mdd.selectCompletePassengerVehicleMeasures()
    } catch(err) {
      console.error(err)
    }
  })
}

async function main5() {
  await runInDamaContext(etl_context, async () => {
    try {
      const mdd = new MDD()
      await mdd.selectAllMeasures()
    } catch(err) {
      console.error(err)
    }
  })
}

async function main6() {
  await runInDamaContext(etl_context, async () => {
    try {
      const mdd = new MDD()
      await mdd.selectIncludeNullValues()
    } catch(err) {
      console.error(err)
    }
  })
}

async function main6() {
  await runInDamaContext(etl_context, async () => {
    try {
      const mdd = new MDD()
      const dd = await mdd.requestNpmrdsDataExport(
        'vt',
        '2024-01-01',
        '2024-01-07',
        true
      )

      console.log(dd)
    } catch(err) {
      console.error(err)
    }
  })
}

main6() ;
