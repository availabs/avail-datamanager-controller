import {
  PgEnv,
  NodePgClient,
  getConnectedNodePgClient,
} from "../dama_db/postgres/PostgreSQL";

import AbstractTasksController from "./AbstractTasksController";

export default class DetachedTasksController extends AbstractTasksController {
  private db_connections: Record<PgEnv, Promise<NodePgClient> | undefined>;

  constructor() {
    super();
    this.db_connections = {};
  }

  protected async getDbConnection(pg_env: PgEnv): Promise<NodePgClient> {
    if (this.db_connections[pg_env]) {
      return <NodePgClient>await this.db_connections[pg_env];
    }

    let done: (db: NodePgClient) => NodePgClient;
    let fail: Function;

    this.db_connections[pg_env] = new Promise((resolve, reject) => {
      // @ts-ignore
      done = resolve;
      fail = reject;
    });

    try {
      const db = await getConnectedNodePgClient(pg_env);

      process.nextTick(() => done(db));

      return db;
    } catch (err) {
      process.nextTick(() => fail(err));
      throw err;
    }
  }

  // NOOP for DetachedTasksController
  protected async releaseDbConnection(): Promise<void> {
    return;
  }
}
