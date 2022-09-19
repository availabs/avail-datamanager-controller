import uuid from "uuid";

import { Context } from "moleculer";

const cleanTypeName = (type: string) =>
  type
    .toLowerCase()
    .replace(/[^0-9a-z_]/, "-")
    .replace(/-+/, "-");

export default function createDataManagerActionCreators(
  ctx: Context,
  dataSourceName: string,
  task: string
) {
  const { params } = ctx;

  const damaEvents = {
    START: `${dataSourceName}/${task}_START`,
    DONE: `${dataSourceName}/${task}_DONE`,
    ERROR: `${dataSourceName}/${task}_ERROR`,
  };

  // @ts-ignore
  const { pg_env } = params || null;

  const id = uuid.v4();

  let start_time: string;
  let end_time: string;

  const start = () => {
    start_time = new Date().toISOString();

    const type = damaEvents.START;

    const eventName = `data_manager.${cleanTypeName(type)}`;

    const event = {
      type,
      payload: params,
      meta: {
        id,
        pg_env,
        start_time,
      },
    };

    ctx.emit(eventName, event);

    return start_time;
  };

  const done = (result: any, damaViewMeta?: Record<string, any>) => {
    end_time = new Date().toISOString();

    const type = damaEvents.DONE;

    const eventName = `data_manager.${cleanTypeName(type)}`;

    const event = {
      type,
      payload: { params, result, damaViewMeta },
      meta: {
        id,
        pg_env,
        start_time,
        end_time,
      },
    };

    ctx.emit(eventName, event);

    return end_time;
  };

  const error = (err: Error) => {
    end_time = new Date().toISOString();

    const type = damaEvents.DONE;

    const eventName = `data_manager.${cleanTypeName(type)}`;

    const event = {
      type,
      payload: err,
      meta: {
        id,
        pg_env,
        start_time,
        end_time,
      },
      error: true,
    };

    ctx.emit(eventName, event);
  };

  return { start, done, error };
}
