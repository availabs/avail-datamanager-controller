import puppeteer, { Browser } from "puppeteer";

import credentials from "../config/transcom_credentials.json";

import logger from "data_manager/logger";

import { sleep } from "data_utils/time";

export default class TranscomAuthTokenCollector {
  _browser_p: Promise<Browser> | null;
  jwt_token_p?: Promise<string>;
  refresh_token_inteval?: ReturnType<typeof setInterval>;

  constructor() {
    this._browser_p = null;
  }

  async getJWT() {
    if (!this._browser_p) {
      await this.start();
    }

    let token: string | undefined;

    while (!(token = await this.jwt_token_p)) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    logger.silly(`TranscomAuthTokenCollector: returning token ${token}`);

    return token;
  }

  private async start() {
    if (!this._browser_p) {
      logger.debug("TranscomAuthTokenCollector: creating puppeteer browser");

      this._browser_p = puppeteer.launch({ headless: true });

      const browser = await this._browser_p;

      const page = await browser.newPage();

      logger.debug("TranscomAuthTokenCollector: creating puppeteer page");

      await page.goto("https://xcmdfe1.xcmdata.org/SSO/#!/login");

      await page.waitForNetworkIdle();

      await page.waitForSelector("#username");


      // Type into search box.
      await page.type("#lgnName", credentials.username);
      await page.type(
        "#loginDiv > div > div.login-inner-box > form > div:nth-child(3) > input",
        credentials.password,
        {
          delay: 100,
        }
      );

      await sleep(1000);

      await page.click(".btn");

      await page.waitForNetworkIdle({
        timeout: 1_000 * 60 * 15,
      });

      logger.debug("TranscomAuthTokenCollector: logged in");

      this.refresh_token_inteval = setInterval(async () => {
        try {
          let retries = 0;
          logger.silly("TranscomAuthTokenCollector: refreshing token");

          await new Promise((resolve) => setTimeout(resolve, 2000));

          this.jwt_token_p = page.evaluate(() => {
            try {
              const usrStr = <string>localStorage.getItem("user");
              const { jwtToken } = JSON.parse(usrStr);
              return jwtToken;
            } catch (err) {
              console.error(err);
            }
          });

          //  If the page.evaluate above returns undefined, it will continue to do so.
          if ((await this.jwt_token_p) === undefined) {
            if (++retries === 10) {
              await this.close();
              return await this.start();
            }
          }

          logger.silly(
            `TranscomAuthTokenCollector: typeof this.jwt_token_p ${typeof this
              .jwt_token_p}`
          );

          logger.silly(
            `TranscomAuthTokenCollector: this.jwt_token_p=${await this
              .jwt_token_p}`
          );
        } catch (err) {
          console.error(err);
        }
      }, 5000);
    }
  }

  async close() {
    if (this._browser_p) {
      clearInterval(this.refresh_token_inteval);

      const browser = await this._browser_p;

      this._browser_p = null;

      await browser.close();
    }
  }
}
