import { createWriteStream } from "fs";
import { inspect } from "util";

import logger from "data_manager/logger";

import {
  Page,
  HTTPRequest,
  HTTPResponse,
  DEFAULT_INTERCEPT_RESOLUTION_PRIORITY,
  ContinueRequestOverrides,
  ErrorCode,
} from "puppeteer";

import { ParsedHttpRequest, ParsedHttpResponse } from "../../domain";

export type HttpRequestInterceptor = (
  event: HTTPRequest
) => void | Promise<void>;

export type ParsedHttpRequestInterceptor = (
  event: ParsedHttpRequest
) => Promise<void>;

export type HttpResponseInterceptor = (
  event: HTTPResponse
) => void | Promise<void>;

export type ParsedHttpResponseInterceptor = (
  event: ParsedHttpResponse
) => Promise<void>;

export default class PageNetworkUtils {
  private _requestInterceptors: Map<
    HttpRequestInterceptor | ParsedHttpRequestInterceptor,
    HttpRequestInterceptor
  >;

  private _responseInterceptors: Map<
    HttpResponseInterceptor | ParsedHttpResponseInterceptor,
    HttpResponseInterceptor
  >;

  private _pendingHttpRequests: Set<HTTPRequest>;
  private _loggingRequests: boolean = false;
  private _loggingResponses: boolean = false;

  static async parseHttpRequestEvent(
    event: HTTPRequest
  ): Promise<ParsedHttpRequest> {
    const url = event.url();
    const method = event.method();

    const postDataStr = event.postData();
    let postData: string | object;

    if (postDataStr) {
      try {
        postData = JSON.parse(postDataStr);
      } catch (err) {
        postData = postDataStr;
      }
    }

    return {
      request_url: url,
      request_method: method,
      // @ts-ignore
      request_body: postData,
      request_event: event,
    };
  }

  static async parseHttpResponseEvent(
    event: HTTPResponse
  ): Promise<ParsedHttpResponse> {
    const request_event = event.request();
    const { request_url, request_method, request_body } =
      await PageNetworkUtils.parseHttpRequestEvent(request_event);

    const response_url = event.url();
    const response_status = event.status();
    const response_from_cache = event.fromCache();

    //  If the response is a redirect, event.text() will throw an error.
    //    See: https://github.com/puppeteer/puppeteer/issues/6390
    let text: string | null = null;

    let response_body = null;

    try {
      text = await event.text();

      if (text) {
        response_body = await event.json();
      }
    } catch (err) {
      //
    }

    // TODO: Add request_timestamp, response_timestamp
    return {
      request_url,
      request_method,
      request_body,
      response_url,
      response_status,
      response_from_cache,
      response_body,
    };
  }

  constructor(private readonly page: Page) {
    this._requestInterceptors = new Map();
    this._responseInterceptors = new Map();
    this._pendingHttpRequests = new Set();
  }

  // SEE: https://github.com/puppeteer/puppeteer/blob/main/docs/api.md#pagesetrequestinterceptionvalue
  async addRequestInterceptor(fn: HttpRequestInterceptor) {
    await this.page.setRequestInterception(true);

    const f = (event: HTTPRequest) => {
      this._pendingHttpRequests.add(event);
      return fn(event);
    };

    this._requestInterceptors.set(fn, f);

    this.page.on("request", f);
  }

  async addParsedRequestInterceptor(
    fn: (event: ParsedHttpRequest) => Promise<void>
  ) {
    await this.page.setRequestInterception(true);

    const f = async (event: HTTPRequest) => {
      logger.silly(`PageNetworkUtils intercepted ${inspect(event)}`);

      const parsedReq = await PageNetworkUtils.parseHttpRequestEvent(event);
      this._pendingHttpRequests.add(event);
      return await fn(parsedReq);
    };

    this._requestInterceptors.set(fn, f);

    this.page.on("request", f);
  }

  async removeRequestInterceptor(
    fn: HttpRequestInterceptor | ParsedHttpRequestInterceptor
  ) {
    const f = this._requestInterceptors.get(fn);

    if (f) {
      this.page.off("request", f);
    }

    this._requestInterceptors.delete(fn);

    if (this._requestInterceptors.size === 0) {
      await this.page.setRequestInterception(false);
    }
  }

  async continuePendingRequest(
    pendingRequest: HTTPRequest | ParsedHttpRequest,
    overrides: ContinueRequestOverrides = {},
    priority: number = DEFAULT_INTERCEPT_RESOLUTION_PRIORITY
  ) {
    // @ts-ignore
    const rawReq: HTTPRequest = pendingRequest.request_event || pendingRequest;

    logger.silly(
      `++++++++++++++++++++++++++++++++++++++++++++++++++++
       \nPageNetworkUtils continuePendingRequest request=${inspect(rawReq, {
         depth: null,
         compact: false,
         sorted: true,
       })}`
    );

    if (!rawReq.isInterceptResolutionHandled()) {
      logger.silly("isInterceptResolutionHandled === false, continuing");

      await rawReq.continue(overrides, priority);
    } else {
      logger.silly("isInterceptResolutionHandled === true, skipping");
    }

    logger.silly("++++++++++++++++++++++++++++++++++++++++++++++++++++");
    this._pendingHttpRequests.delete(rawReq);
  }

  async continueAllPendingRequests() {
    const pending = [...this._pendingHttpRequests];
    for (const req of pending) {
      await this.continuePendingRequest(req);
    }
  }

  async abortPendingRequest(
    pendingRequest: HTTPRequest | ParsedHttpRequest,
    errorCode: ErrorCode = "failed",
    priority: number = DEFAULT_INTERCEPT_RESOLUTION_PRIORITY
  ) {
    // @ts-ignore
    const rawReq: HTTPRequest = pendingRequest?.request_event || pendingRequest;

    if (!rawReq.isInterceptResolutionHandled()) {
      await rawReq.abort(errorCode, priority);
    }

    this._pendingHttpRequests.delete(rawReq);
  }

  async abortAllPendingRequests() {
    const pending = [...this._pendingHttpRequests];
    for (const req of pending) {
      await this.abortPendingRequest(req);
    }
  }

  addResponseInterceptor(fn: HttpResponseInterceptor) {
    this._responseInterceptors.set(fn, fn);

    this.page.on("response", fn);
  }

  addParsedResponseInterceptor(
    fn: (event: ParsedHttpResponse) => Promise<void>
  ) {
    const f = async (event: HTTPResponse) => {
      const parsedRes = await PageNetworkUtils.parseHttpResponseEvent(event);
      return await fn(parsedRes);
    };

    this._responseInterceptors.set(fn, f);

    this.page.on("response", f);
  }

  removeResponseInterceptor(
    fn: HttpResponseInterceptor | ParsedHttpResponseInterceptor
  ) {
    const f = this._responseInterceptors.get(fn);

    if (f) {
      this.page.off("response", f);
    }

    this._responseInterceptors.delete(fn);
  }

  async logResponses(path: string) {
    if (this._loggingResponses) {
      return;
    }
    this._loggingResponses = true;

    const logStream = createWriteStream(path, { flags: "a" });
    // const ws = createWriteStream(path);
    // const zip = createGzip();

    // zip.pipe(ws);

    this.page.on("response", async (event: HTTPResponse) => {
      const res = await PageNetworkUtils.parseHttpResponseEvent(event);

      const d = { ...res, request_event: undefined };

      const good = logStream.write(`${JSON.stringify(d)}\n`);

      if (!good) {
        await new Promise<void>((resolve) => {
          logStream.once("drain", resolve);
          setTimeout(() => {
            logStream.off("drain", resolve);
            resolve();
          }, 250);
        });
      }
    });
  }

  async logRequests(path: string) {
    if (this._loggingRequests) {
      return;
    }

    this._loggingRequests = true;

    const logStream = createWriteStream(path, { flags: "a" });
    // const ws = createWriteStream(path);
    // const logStream = createGzip();

    // logStream.pipe(ws);

    const fn = async (event: HTTPRequest) => {
      const req = await PageNetworkUtils.parseHttpRequestEvent(event);

      const d = { ...req, request_event: undefined };

      const good = logStream.write(`${JSON.stringify(d)}\n`);

      if (!good) {
        await new Promise<void>((resolve) => {
          logStream.once("drain", resolve);
          setTimeout(() => {
            logStream.off("drain", resolve);
            resolve();
          }, 250);
        });
      }
    };

    this.page.on("request", fn);
  }
}
