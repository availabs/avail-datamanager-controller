import puppeteer, { Browser } from "puppeteer";

import credentials from "../../config/transcom_credentials.json";

import logger from "data_manager/logger";

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

    logger.debug(`TranscomAuthTokenCollector: returning token ${token}`);

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
      await page.waitForSelector("#username");

      // Type into search box.
      await page.type("#username", credentials.username);
      await page.type(
        "#loginDiv > div > div.login-inner-box > form > div:nth-child(3) > input",
        credentials.password
      );
      await page.click(".btn");

      await page.waitForNetworkIdle();

      logger.debug("TranscomAuthTokenCollector: logged in");

      this.refresh_token_inteval = setInterval(async () => {
        try {
          logger.silly("TranscomAuthTokenCollector: refreshing token");

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
            await this.close();
            await this.start();
          }

          logger.debug(
            `TranscomAuthTokenCollector: typeof this.jwt_token_p ${typeof this
              .jwt_token_p}`
          );

          logger.debug(
            `TranscomAuthTokenCollector: this.jwt_token_p=${await this
              .jwt_token_p}`
          );
        } catch (err) {
          console.error(err);
        }
      }, 1000);
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
