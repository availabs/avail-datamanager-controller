import { mkdirSync } from "fs";
import { join } from "path";

import puppeteer, { Browser, Page } from "puppeteer";

import browser_options from "../../config/browser_options";

const diskCacheDir = join(__dirname, "../../config/disk-cache");

mkdirSync(diskCacheDir, { recursive: true });

let browser: Browser | null = null;

export async function getBrowser(): Promise<Browser> {
  if (browser !== null) {
    return browser;
  }

  const {
    headless = process.env.NODE_ENV !== "development",
    windowHeight = 600,
    windowWidth = 900,
  } = browser_options;

  browser = await puppeteer.launch({
    headless,
    ignoreHTTPSErrors: true,
    // https://stackoverflow.com/a/55302448/3970755
    defaultViewport: null,
    args: [
      "--disable-dev-shm-usage",
      "--mute-audio",
      "--disable-gpu",
      `--disk-cache-dir=${diskCacheDir}`,
      `--window-size=${windowWidth},${windowHeight}`,
    ],
  });

  return browser;
}

export async function closeBrowser() {
  if (browser !== null) {
    const b = browser;
    browser = null;
    await b.close();
  }
}

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
