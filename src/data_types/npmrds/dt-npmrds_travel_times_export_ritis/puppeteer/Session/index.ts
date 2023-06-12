import { existsSync, readFileSync, writeFileSync } from "fs";

import { join } from "path";

import { Page } from "puppeteer";

import { sleep } from "data_utils/time";

import credentials from "../../config/credentials";

import { getBrowser } from "../WebBrowser";

export const loginPageUrlRE = /^https:\/\/npmrds.ritis.org\/analytics\/$/;

const ritisLandingPageUrl = "https://npmrds.ritis.org/analytics/";

const cookiesPath = join(__dirname, "../../config/", "cookies.json");

let cookies = existsSync(cookiesPath)
  ? JSON.parse(readFileSync(cookiesPath, { encoding: "utf8" }))
  : [];

// https://stackoverflow.com/a/48035121/3970755
export async function createPage(): Promise<Page> {
  const b = await getBrowser();
  const page = await b.newPage();

  // https://stackoverflow.com/a/53417695/3970755
  page.setViewport({
    width: 0,
    height: 0,
  });

  return page;
}

async function isLoginPage(page: Page) {
  await page.waitForNetworkIdle();

  try {
    const signInFormSelector = "#signInFormWrapper";

    // Throws if selector not found.
    await page.waitForSelector(signInFormSelector, {
      visible: true,
      timeout: 500,
    });
    return true;
  } catch (err) {
    // Throws if login form not on page... i.e. logged in.
  }

  return false;
}

export async function getSessionPage(): Promise<Page> {
  const page = await createPage();

  // https://github.com/puppeteer/puppeteer/issues/1599#issuecomment-355473214
  // https://github.com/puppeteer/puppeteer/issues/8640
  // https://stackoverflow.com/a/47291149
  const client = await page.target().createCDPSession();
  await client.send("Network.enable", {
    maxResourceBufferSize: 1024 * 1204 * 100,
    maxTotalBufferSize: 1024 * 1204 * 200,
  });

  await page.setCookie(...cookies);

  await page.goto(ritisLandingPageUrl);

  if (!(await isLoginPage(page))) {
    return page;
  }

  const usernameInputSelector = "#username";
  const passwordInputSelector = "#password";

  // https://community.auth0.com/t/scripting-with-puppeteer/12662/2
  await page.waitForSelector(usernameInputSelector);
  await page.waitForSelector(passwordInputSelector);

  await sleep(500);

  await page.type(usernameInputSelector, credentials.email, {
    delay: 50,
  });

  await sleep(500);

  await page.type(passwordInputSelector, credentials.password, {
    delay: 50,
  });

  await sleep(500);

  await page.keyboard.press("Enter");

  await page.waitForNavigation({ waitUntil: "networkidle2" });

  cookies = await page.cookies();

  writeFileSync(cookiesPath, JSON.stringify(cookies));

  return page;
}
