import { Service } from "moleculer";
import { PgEnv, NodePgPoolClient } from "../dama_db/postgres/PostgreSQL";

import AbstractTasksController from "./AbstractTasksController";

export default class AttachedTasksController extends AbstractTasksController {
  constructor(private readonly moleculer_service: Service) {
    super();
  }

  // Allow getting connections through dama_db or directly when within detached tasks.
  protected async getDbConnection(pg_env: PgEnv): Promise<NodePgPoolClient> {
    const db = <NodePgPoolClient>(
      await this.moleculer_service.broker.call(
        "dama_db.getDbConnection",
        {},
        { meta: { pgEnv: pg_env } }
      )
    );

    return db;
  }

  protected async releaseDbConnection(db: NodePgPoolClient): Promise<void> {
    return db.release();
  }
}
