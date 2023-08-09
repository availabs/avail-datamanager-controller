import _ from "lodash";

import dedent from "dedent";
import pgFormat from "pg-format";

import DamaContextAttachedResource from "data_manager/contexts";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { NodePgQueryConfig } from "../dama_db/postgres/PostgreSQL";
import { DamaSource, DamaSourceName, DamaViewID } from "./domain";

type TableDescription = Record<
  string,
  { column_type: string; column_number: number }
>;

function warnAboutAdditionalMetadataProps(
  meta_props: string[],
  insert_cols: string[]
) {
  const xtraMetaProps = _.difference(meta_props, insert_cols).filter(
    (c) => c !== "source_dependencies_names"
  );

  if (meta_props.includes("source_dependencies")) {
    console.warn(
      'DamaSource source_dependencies cannot be specified in the metadata. Use "source_dependencies_names".'
    );
  }

  if (xtraMetaProps.length) {
    console.warn(
      `The following DamaSource properties cannot be inserted into data_manager.sources: ${xtraMetaProps}.`
    );
  }
}

export type LoadDataSourcesQueries = {
  name: string;
  exists_query: string | NodePgQueryConfig;
  insert_query: string | NodePgQueryConfig;
  all_source_dependency_names: null | string[];
  existing_source_dependency_names_query: null | NodePgQueryConfig;
  update_source_dependencies_query: null | NodePgQueryConfig;
};

export type ToposortedLoadDataSourcesQueries = LoadDataSourcesQueries[];

class DamaMeta extends DamaContextAttachedResource {
  /**
   * Get a description of a database table's schema.
   *
   * @remarks
   *    The table must either be
   *      * declared in the data_manager.views using the table_schema and table_name columns
   *      * in the data_manager or _data_manager_admin schemas
   *
   * @param table_schema - The database schema name.
   *
   * @param table_name - The database table name.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an Object whose keys are the column names and values are \{ column_type, column_number \}.
   *
   * @throws Throws an Error if no such table exists.
   */
  async describeTable(
    table_schema: string,
    table_name: string,
    pg_env = this.pg_env
  ): Promise<TableDescription> {
    const text = dedent(`
      SELECT
          column_name,
          column_type,
          column_number
        FROM _data_manager_admin.table_column_types
        WHERE (
          ( table_schema = $1 )
          AND
          ( table_name = $2 )
        )
      ;
    `);

    const values = [table_schema, table_name];

    const { rows } = await dama_db.query(
      {
        text,
        values,
      },
      pg_env
    );

    if (rows.length === 0) {
      const table = pgFormat("%I.%I", table_schema, table_name);

      logger.debug(`No such table in data_manager: ${table}`);
      logger.debug(JSON.stringify({ text, values }, null, 4));

      throw new Error(`No such table in data_manager: ${table}`);
    }

    const table_description = rows.reduce(
      (acc: any, { column_name, column_type, column_number }) => {
        acc[column_name] = { column_type, column_number };
        return acc;
      },
      {}
    );

    return table_description;
  }

  /**
   * Get list of a database table's columns.
   *
   * @param table_schema - The database schema name.
   *
   * @param table_name - The database table name.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns An array of the table columns.
   *
   * @throws Throws an Error if no such table exists.
   */
  async getTableColumns(
    table_schema: string,
    table_name: string,
    pg_env = this.pg_env
  ) {
    const table_description = await this.describeTable(
      table_schema,
      table_name,
      pg_env
    );

    const column_names = Object.keys(table_description).sort(
      (a, b) =>
        table_description[a].column_number - table_description[b].column_number
    );

    return column_names;
  }

  /**
   * Get the table_schema and table_name of a DamaView.
   *
   * @remarks
   *    Uses the data_manager.views.table_schema and data_manager.views.table_name columns.
   *
   * @param dama_view_id - The DamaView's ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an \{ table_schema, table_name \} object.
   *
   * @throws Throws an Error if no such DamaView exists.
   */
  async getDamaViewTableSchemaAndName(
    dama_view_id: number,
    pg_env = this.pg_env
  ): Promise<{ table_schema: string; table_name: string }> {
    const text = dedent(`
      SELECT
          table_schema,
          table_name
        FROM data_manager.views
        WHERE ( view_id = $1 )
      ;
    `);

    const { rows } = await dama_db.query(
      { text, values: [dama_view_id] },
      pg_env
    );

    if (rows.length === 0) {
      throw new Error(`Invalid DamaViewID: ${dama_view_id}`);
    }

    return rows[0];
  }

  /**
   * Get a list of the column names for a DamaView.
   *
   * @remarks
   *    Uses the data_manager.views.table_schema and data_manager.views.table_name columns.
   *
   * @param dama_view_id - The DamaView's ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an \{ table_schema, table_name \} object.
   *
   * @throws Throws an Error if no such DamaView exists,
   *    or if the DamaView's (table_schema, table_name) do not exist.
   */
  async getDamaViewTableColumns(
    dama_view_id: number,
    pg_env = this.pg_env
  ): Promise<string[]> {
    const { table_schema, table_name } =
      await this.getDamaViewTableSchemaAndName(dama_view_id, pg_env);

    return this.getTableColumns(table_schema, table_name, pg_env);
  }

  /**
   * Get the DamaSource ID for the DamaSource name.
   *
   * @remarks
   *    There are NON NULL and UNIQUE contraints on the data_manager.sources.name column.
   *
   * @param dama_source_name - The DamaSource's name.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the DamaSource's ID.
   *
   * @throws Throws an Error if no such DamaSource exists.
   */
  async getDamaSourceIdForName(
    dama_source_name: string,
    pg_env = this.pg_env
  ): Promise<number> {
    const text = dedent(`
      SELECT
          source_id
        FROM data_manager.sources
        WHERE ( name = $1 )
      ;
    `);

    const { rows } = await dama_db.query(
      {
        text,
        values: [dama_source_name],
      },
      pg_env
    );

    if (rows.length === 0) {
      throw new Error(`No DamaSource with name ${dama_source_name}.`);
    }

    const [{ source_id }] = rows;

    return source_id;
  }

  /**
   * Generate a node-postgres parameterized query to INSERT the row into the table.
   *
   * @remarks
   *    Maps row object keys to database column names.
   *    Silently ignores properties without corresponding columns.
   *
   *    NOTE: "RETURNING *" is added to the INSERT statement.
   *
   *    NOTE: Converts empty strings to NULL.
   *
   * @param table_schema - The database schema name.
   *
   * @param table_name - The database table name.
   *
   * @param row - The row to INSERT.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns a node-postgres parameterized query
   *    See: https://node-postgres.com/features/queries#parameterized-query
   *
   * @throws Throws an Error if the specified table does not exist.
   */
  async generateInsertStatement(
    table_schema: string,
    table_name: string,
    row: Record<string, any>,
    pg_env = this.pg_env
  ): Promise<NodePgQueryConfig> {
    const new_row_keys = Object.keys(row);

    const table_description = await this.describeTable(
      table_schema,
      table_name,
      pg_env
    );

    const table_columns = Object.keys(table_description);

    const insert_stmt_cols = _.intersection(new_row_keys, table_columns);

    const insert_stmt_obj = insert_stmt_cols.reduce(
      (acc, col, i) => {
        acc.format_types.push("%I");
        acc.format_values.push(col);

        const { column_type } = table_description[col];

        acc.placeholders.push(`$${i + 1}::${column_type}`);

        let v = row[col];

        if (v === "") {
          v = null;
        }

        if (Array.isArray(v) && /^json/i.test(column_type)) {
          v = JSON.stringify(v);
        }

        acc.values.push(v);

        return acc;
      },
      {
        format_types: <string[]>[],
        format_values: <string[]>[],
        placeholders: <string[]>[],
        values: <any[]>[],
      }
    );

    const text = dedent(
      pgFormat(
        `
          INSERT INTO %I.%I (
              ${insert_stmt_obj.format_types}
            ) VALUES (${insert_stmt_obj.placeholders})
            RETURNING *
          ;
        `,
        table_schema,
        table_name,
        ...insert_stmt_obj.format_values
      )
    );

    const { values } = insert_stmt_obj;

    return { text, values };
  }

  /**
   * INSERT the row into the specified table.
   *
   * @remarks
   *    Maps row object keys to database column names.
   *    Silently ignores properties without corresponding columns.
   *
   * @param table_schema - The database schema name.
   *
   * @param table_name - The database table name.
   *
   * @param row - The row to INSERT.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns The INSERT result. The INSERTed row can be found at result.rows[0].
   *    See: https://node-postgres.com/apis/result
   *
   * @throws Throws an Error if the specified table does not exist.
   */
  async insertNewRow(
    table_schema: string,
    table_name: string,
    row: Record<string, any>,
    pg_env = this.pg_env
  ) {
    const q = await this.generateInsertStatement(
      table_schema,
      table_name,
      row,
      pg_env
    );

    return dama_db.query(q, pg_env);
  }

  /**
   * Get the MAX DamaView ID for the DamaSource.
   *
   * @param dama_source_id - The DamaSource ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns The MAX DamaView ID
   *
   * @throws Throws an Error if the specified DamaSource does not exist.
   */
  async getDataSourceMaxViewId(
    dama_source_id: number,
    pg_env = this.pg_env
  ): Promise<number> {
    const text = dedent(`
      SELECT
          MAX(view_id) AS latest_view_id
        FROM data_manager.views
        WHERE ( source_id = $1 )
      ;
    `);

    const { rows } = await dama_db.query(
      {
        text,
        values: [dama_source_id],
      },
      pg_env
    );

    if (rows.length < 1) {
      throw new Error(`No DamaView for DamaSource ${dama_source_id}`);
    }

    const [{ latest_view_id }] = rows;

    return latest_view_id;
  }

  /**
   * Get a list of the database table columns for the latest DamaView (max view_id) for the DamaSource.
   *
   * @remarks
   *    Uses the data_manager.views.table_schema and data_manager.views.table_name columns.
   *
   * @param dama_source_id - The DamaSource ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns The MAX DamaView ID
   *
   * @throws Throws an Error if
   *    * the specified DamaSource does not exist, or
   *    * the latest DamaView table does not exist.
   */
  async getDataSourceLatestViewTableColumns(
    dama_source_id: number,
    pg_env = this.pg_env
  ) {
    const dama_view_id = await this.getDataSourceMaxViewId(
      dama_source_id,
      pg_env
    );

    return this.getDamaViewTableColumns(dama_view_id, pg_env);
  }

  /**
   * DEPRECATED Generate a node-postgres parameterized query to
   *    INSERT the new_dama_view object into the data_manager.views table.
   *
   * @deprecated
   *    Use createNewDamaView instead.
   *
   * @param new_dama_view - An object representing the new DamaView to INSERT.
   *    MUST include either a source_id or a dama_source_name property.
   *    The dama_source_name property is a convenience that allows hard-coding known DamaSource names
   *      rather than querying the DamaSource ID for a given PgEnv.
   *      dama_source_name is not INSERTed into the data_manager.views table.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns a node-postgres parameterized query
   *    See: https://node-postgres.com/features/queries#parameterized-query
   */
  async getInsertDamaViewRowQuery(
    new_dama_view: Record<string, any>,
    pg_env = this.pg_env
  ) {
    const column_names: string[] = await this.getTableColumns(
      "data_manager",
      "views",
      pg_env
    );

    const new_row = _.pick(new_dama_view, column_names);

    new_row.etl_context_id = new_row.etl_context_id || null;

    if (!new_row.etl_context_id) {
      try {
        new_row.etl_context_id = this.etl_context_id;
      } catch (err) {
        //
      }
    }

    const cols: string[] = [];
    const queryParams: any[] = [];

    const selectClauses = Object.keys(new_row)
      .map((k) => {
        if (k === "view_id" || k === "source_id") {
          return null;
        }

        cols.push(k);

        let val = new_row[k];

        if (val === "") {
          val = null;
        }

        const i = queryParams.push(val);

        if (k === "last_updated") {
          return `$${i}::TIMESTAMP AS ${k}`;
        }

        if (/_date$/.test(k)) {
          return `$${i}::DATE AS ${k}`;
        }

        return `$${i} AS ${k}`;
      })
      .filter(Boolean)
      .join(", ");

    // FIXME: remove the dama_source_name requirement. Just use source_id.
    const { data_source_name } = new_dama_view;
    const data_source_name_param_num = queryParams.push(data_source_name);

    const sql = `
      INSERT INTO data_manager.views (
        source_id, ${cols.join(", ")}
      )
        SELECT
            a.source_id,
            ${selectClauses}
          FROM data_manager.sources AS a
          WHERE ( name = $${data_source_name_param_num} )
        RETURNING *
      ;
    `;

    return {
      text: dedent(sql),
      values: queryParams,
    };
  }

  /**
   * DEPRECATED INSERT the new_dama_view object into the data_manager.views table.
   *
   * @deprecated Use createNewDamaView instead
   *
   * @param new_dama_view - An object representing the new DamaView to INSERT.
   *    MUST include either a source_id or a dama_source_name property.
   *    The dama_source_name property is a convenience that allows hard-coding known DamaSource names
   *      rather than querying the DamaSource ID for a given PgEnv.
   *      dama_source_name is not INSERTed into the data_manager.views table.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the new DamaView's ID
   */
  async insertNewDamaView(
    new_dama_view: Record<string, any>,
    pg_env = this.pg_env
  ) {
    const query = await this.getInsertDamaViewRowQuery(new_dama_view, pg_env);

    const {
      rows: [inserted],
    } = await dama_db.query(query, pg_env);

    const { view_id: dama_view_id } = inserted;

    logger.debug(`dama_meta: Created new view ${dama_view_id}`);
    logger.silly(`dama_meta: ${JSON.stringify(inserted, null, 4)}`);

    return { dama_view_id };
  }

  /**
   * Get the source_id and name of all DamaSources in the PgEnv.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an Array of \{ source_id, name \}
   */
  async getDamaDataSources(pg_env = this.pg_env) {
    const sql = dedent(`
      SELECT
          source_id,
          name
        FROM data_manager.sources
        ORDER BY 1
      ;
    `);

    const { rows } = await dama_db.query(sql, pg_env);

    return rows;
  }

  /**
   * Get the JSON Schema description of the JSON representation of a table's data.
   *
   * @remarks
   *    The JSON Schema describes the object node-pg creates for the table's rows.
   *      See:
   *        * https://json-schema.org/
   *        * https://node-postgres.com/features/types
   *
   * @param table_schema - The database schema name.
   *
   * @param table_name - The database table name.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns a JSON Schema object
   */
  async getTableJsonSchema(
    table_schema: string,
    table_name: string,
    pg_env = this.pg_env
  ) {
    const text = dedent(`
      SELECT
          t.schema
        FROM _data_manager_admin.table_json_schema
        WHERE (
          ( table_schema = $1 )
          AND
          ( table_name = $2 )
        )
      ;
    `);

    const { rows } = await dama_db.query(
      {
        text,
        values: [table_schema, table_name],
      },
      pg_env
    );

    if (rows.length === 0) {
      const table_full_name = pgFormat("%I.%I", table_schema, table_name);

      throw new Error(`No DamaView with table ${table_full_name}`);
    }

    const [{ schema }] = rows;

    return schema;
  }

  /**
   * Get properties of a DamaView
   *
   * @remarks
   *    The properties MUST map exactly to data_manager.views columns.
   *
   * @param dama_view_id - The DamaView ID.
   *
   * @param properties - The list of properties to obtain the values of
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an Array of objects where the object keys are the passed properties.
   */
  async getDamaViewProperties(
    dama_view_id: number,
    properties: string | string[],
    pg_env = this.pg_env
  ) {
    properties = Array.isArray(properties) ? properties : [properties];

    const formatObj = properties.reduce(
      (acc: any, prop: string) => {
        acc.format_types.push("%I");
        acc.format_values.push(prop);
        return acc;
      },
      { format_types: [], format_values: [] }
    );

    const sql = dedent(
      pgFormat(
        `
          SELECT
              ${formatObj.format_types}
            FROM _data_manager_admin.dama_views_comprehensive
            WHERE ( view_id = $1 )
        `,
        ...formatObj.format_values
      )
    );

    const { rows } = await dama_db.query(
      {
        text: sql,
        values: [dama_view_id],
      },
      pg_env
    );

    if (rows.length !== 1) {
      throw new Error(`Invalid DamaViewID ${dama_view_id}`);
    }

    return rows[0];
  }

  /**
   *  Get the standardized name for a DamaView.
   *
   * @remarks
   *    The data_manager.views tables does NOT have a name column.
   *
   *    The name returned by this function is generated in the database for internal use.
   *      The primary use of the dama_view_name is automated database table naming.
   *      The generated names offer some human readability when inspecting the database
   *        or creating external files such as exported CSVs or MBtiles.
   *
   *    The dama_view_name generation rules:
   *      (1) A dama_view_name begins with s\<source_id\>_v\<view_id\>
   *            For example, for DamaSourceID 10 and DamaViewID 101,
   *              the dama_view_name prefix would be s10_v101.
   *      (2) The DamaView's DamaSourceName is converted to snake_case and appended to (1).
   *      (3) The string obtained in (2) is truncated to 50 characters.
   *            This is because PostgreSQL limits database object names to 63 characters
   *              and extra characters are reserved for suffixes when naming indices, triggers, etc.
   *
   *    NOTE: DamaSourceNames are NOT immutable.
   *          Therefore the s\<source_id\>_v\<view_id\> is the only reliable way to associate
   *            database objects with their respective DamaSource and DamaView.
   *
   * @param dama_view_id - The DamaView ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the database-generated dama_view_name
   */
  async getDamaViewName(dama_view_id: number, pg_env = this.pg_env) {
    const { dama_view_name } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_view_name",
      pg_env
    );

    return dama_view_name;
  }

  /**
   *  Get the prefix of a DamaView's standardized name.
   *
   * @remarks
   *    A dama_view_name begins with s\<source_id\>_v\<view_id\>
   *      For example, for DamaSourceID 10 and DamaViewID 101,
   *        the dama_view_name prefix would be s10_v101.
   *
   * @param dama_view_id - The DamaView ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the s\<source_id\>_v\<view_id\> prefix of database-generated DamaView names.
   */
  async getDamaViewNamePrefix(dama_view_id: number, pg_env = this.pg_env) {
    const { dama_view_name_prefix } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_view_name_prefix",
      pg_env
    );

    return dama_view_name_prefix;
  }

  /**
   *  Get the DamaView's unique identifier across all PgEnvs.
   *
   * @remarks
   *    The DamaView's global ID is a unique identifier across all PgEnvs.
   *    DamaView global IDs are prefixed with the PgEnv's database_id.
   *      The database_id is a UUID created upon database initialization.
   *
   * @param dama_view_id - The DamaView ID.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an Array of objects where the object keys are the passed properties.
   */
  async getDamaViewGlobalId(dama_view_id: number, pg_env = this.pg_env) {
    const { dama_global_id } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_global_id",
      pg_env
    );

    return dama_global_id;
  }

  /**
   * DEPRECATED
   */
  async getDamaViewMapboxPaintStyle(
    dama_view_id: number,
    pg_env = this.pg_env
  ) {
    const { mapbox_paint_style } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_global_id",
      pg_env
    );

    return mapbox_paint_style;
  }

  /**
   * Generate the parameterized query to create a new DamaSource
   *
   * @remarks
   *    In most cases, you'll want to use createNewDamaSource.
   *
   * @param new_row - Object describing the new DamaSource.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the newly created DamaSource.
   */
  async generateCreateDamaSourceQuery(
    new_row: Record<string, any>,
    pg_env = this.pg_env
  ) {
    logger.debug("generateCreateDamaSourceQuery", new_row, pg_env);

    return this.generateInsertStatement(
      "data_manager",
      "sources",
      new_row,
      pg_env
    );
  }

  /**
   * Create a new DamaSource
   *
   * @param new_row - Object describing the new DamaSource.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the newly created DamaSource.
   */
  async createNewDamaSource(
    new_row: Record<string, any>,
    pg_env = this.pg_env
  ) {
    const q = await this.generateCreateDamaSourceQuery(new_row, pg_env);

    const {
      rows: [new_dama_source],
    } = await dama_db.query(q, pg_env);

    logger.info(
      `dama_meta: Created new DamaSource ${new_dama_source.source_id}`
    );

    logger.silly(`dama_meta: ${JSON.stringify(new_dama_source, null, 4)}`);

    return new_dama_source;
  }

  /**
   * Generate the parameterized query to create a new DamaView
   *
   * @remarks
   *    In most cases, you'll want to use createNewDamaView.
   *
   * @param new_row - Object describing the new DamaView.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the newly created DamaView.
   */
  async generateCreateDamaViewSql(
    new_row: Record<string, any>,
    pg_env = this.pg_env
  ) {
    return this.generateInsertStatement(
      "data_manager",
      "views",
      new_row,
      pg_env
    );
  }

  /**
   * Create a new DamaView
   *
   * @param new_row - Object describing the new DamaView.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the newly created DamaView.
   */
  async createNewDamaView(new_row: Record<string, any>, pg_env = this.pg_env) {
    const q = await this.generateCreateDamaViewSql(new_row, pg_env);

    const {
      rows: [new_dama_view],
    } = await dama_db.query(q, pg_env);

    logger.info(`dama_meta: Created new DamaView ${new_dama_view.view_id}`);
    logger.silly(`dama_meta: ${JSON.stringify(new_dama_view, null, 4)}`);

    return new_dama_view;
  }

  /**
   * Delete a DamaSource
   *
   * @remarks
   *    NOTE: Also deletes all DamaViews for the DamaSource
   *
   * @param dama_source_id - The ID of the DamaSource to delete
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the result of the DELETE FROM dama_manager.sources command.
   */
  async deleteDamaSource(dama_source_id: number, pg_env = this.pg_env) {
    const deleteViews = `DELETE FROM data_manager.views where source_id = ${dama_source_id}`;
    const deleteSource = `DELETE FROM data_manager.sources where source_id = ${dama_source_id}`;

    const [, , res] = await dama_db.query(
      ["BEGIN ;", deleteViews, deleteSource, "COMMIT ;"],
      pg_env
    );

    logger.info("dama_meta deleteDamaSource", res);

    return res;
  }

  /**
   * Delete a DamaView
   *
   * @param dama_view_id - The ID of the DamaView to delete
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns the result of the DELETE FROM dama_manager.views command.
   */
  async deleteDamaView(dama_view_id: number, pg_env = this.pg_env) {
    const sql = `DELETE FROM data_manager.views where view_id = ${dama_view_id}`;

    const res = await dama_db.query(sql, pg_env);

    logger.info("dama_meta deleteDamaView", res);

    return sql;
  }

  /**
   * Make a DamaView authoritative.
   *
   * @remarks
   *    Uses an 'authoritative' property on the data_manager.views.meta JSONB column.
   *
   * @param dama_view_id - The ID of the DamaView to delete
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns "success" if successful
   */
  async makeAuthoritativeDamaView(dama_view_id: number, pg_env = this.pg_env) {
    const makeViewAuthSql = `
              UPDATE data_manager.views
              set metadata = CASE WHEN metadata is null THEN '{"authoritative": "true"}' ELSE metadata::text::jsonb || '{"authoritative": "true"}'  END
              where view_id = ${dama_view_id};
        `;

    const invalidateOtherViewsSql = `
              UPDATE data_manager.views
              set metadata = metadata || '{"authoritative": "false"}'
              where source_id IN (select source_id from data_manager.views where view_id = ${dama_view_id})
              and view_id != ${dama_view_id};`;

    await dama_db.query(
      ["BEGIN ;", makeViewAuthSql, invalidateOtherViewsSql, "COMMIT ;"],
      pg_env
    );

    return "success";
  }

  /**
   * Get DamaSource metadata (all columns of the dama_manager.sources table) for the DamaSource names.
   *
   * @param dama_source_names - The names of the DamaSources whose metadata to return.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an object with DamaSources names as keys and DamaSources as values.
   */
  async getDamaSourceMetadataByName(
    dama_source_names: string[],
    pg_env = this.pg_env
  ): Promise<Record<DamaSourceName, DamaSource>> {
    const text = dedent(`
      SELECT
          *
        FROM data_manager.sources
        WHERE ( name = ANY($1) )
    `);

    const { rows } = await dama_db.query(
      {
        text,
        values: [dama_source_names],
      },
      pg_env
    );

    const rowsByName = rows.reduce((acc: Record<string, any>, row: any) => {
      const { name } = row;

      acc[name] = row;

      return acc;
    }, {});

    const metaByName = dama_source_names.reduce(
      (acc: Record<string, any>, name: string) => {
        acc[name] = rowsByName[name] || null;

        return acc;
      },
      {}
    );

    return metaByName;
  }

  /**
   * Internal method used by loadToposortedDamaSourceMetadata
   */
  protected async generateToposortedLoadDataSourcesQueries(
    toposorted_dama_sources_meta: Record<string, any>[],
    pg_env = this.pg_env
  ): Promise<ToposortedLoadDataSourcesQueries> {
    const table_cols = await this.getTableColumns(
      "data_manager",
      "sources",
      pg_env
    );

    const insertable_cols = table_cols.filter(
      (c) => c !== "source_dependencies"
    );

    const stmts: ToposortedLoadDataSourcesQueries = [];

    for (const dama_src_meta of toposorted_dama_sources_meta) {
      // @ts-ignore
      const {
        name,
        source_dependencies_names,
      }: {
        name: string;
        source_dependencies_names: null | string[] | string[][];
      } = dama_src_meta;

      const exists_query = dedent(
        pgFormat(
          `
            SELECT EXISTS (
              SELECT
                  1
                FROM data_manager.sources
                WHERE ( name = %L )
            ) AS data_source_exists ;
          `,
          name
        )
      );

      const meta_props = Object.keys(dama_src_meta);
      const insert_cols = _.intersection(meta_props, insertable_cols);
      const insert_values: any[] = [];

      warnAboutAdditionalMetadataProps(meta_props, insert_cols);

      const insert_params: string[] = [];

      for (const col of insert_cols) {
        insert_values.push(dama_src_meta[col]);
        insert_params.push(`$${insert_values.length}`);
      }

      const col_format_strs = insert_cols.map(() => "%I");

      const insert_text = dedent(
        pgFormat(
          `
            INSERT INTO data_manager.sources( ${col_format_strs.join(", ")} )
              VALUES ( ${insert_params.join(", ")} )
              ON CONFLICT DO NOTHING
              RETURNING * ;
          `,
          ...insert_cols
        )
      );

      const insert_query = {
        text: insert_text,
        values: insert_values,
      };

      //  Here we UPDATE the source_dependencies column.
      //
      //    This MUST be done after the INSERT because some DamaSources,
      //      such as the NpmrdsAuthoritativeTravelTimesDb,
      //      use table inheritance/partitioning and therefore
      //      are represented as a (recursive) Tree data structure.
      //
      //        * at the leaves, the source_dependencies are NpmrdsTravelTimesExportDb
      //        * at the internal nodes, the source_dependencies are NpmrdsAuthoritativeTravelTimesDb
      //
      let all_source_dependency_names: null | string[] = null;
      let existing_source_dependency_names_query: null | NodePgQueryConfig =
        null;

      let update_source_dependencies_query: null | NodePgQueryConfig = null;

      if (
        Array.isArray(source_dependencies_names) &&
        source_dependencies_names.length
      ) {
        const update_values: any = [];

        let source_dependencies_subquery: string;

        //  source_dependencies arrays can be 1 or 2 dimensional arrays of source_ids
        //    1 dimensional arrays of source_ids represent (a AND b)
        //    2 dimensional arrays represent ((a AND b) OR (c AND d))
        if (Array.isArray(source_dependencies_names[0])) {
          const nestedArraySubqueries: string[] = [];

          for (const sourceDepNamesArr of source_dependencies_names) {
            update_values.push(sourceDepNamesArr);

            nestedArraySubqueries.push(`
              (
                SELECT
                    array_agg(source_id ORDER BY source_id) AS deps
                  FROM data_manager.sources
                  WHERE ( name = ANY( $${update_values.length} ) )
              )`);
          }

          source_dependencies_subquery = `
            ARRAY[
              ${nestedArraySubqueries.join(",\n")}
            ]
        `;
        } else {
          update_values.push(source_dependencies_names);
          source_dependencies_subquery = `
            SELECT
                array_agg(source_id ORDER BY source_id) AS deps
              FROM data_manager.sources
              WHERE ( name = ANY( $${update_values.length} ) )
          `;
        }

        update_values.push(name);

        const updateText = dedent(`
          UPDATE data_manager.sources
            SET source_dependencies = (
              ${source_dependencies_subquery}
            )
            WHERE ( name = $${update_values.length} )
            RETURNING *
          ;
        `);

        update_source_dependencies_query = {
          text: updateText,
          values: update_values,
        };

        all_source_dependency_names = _(source_dependencies_names)
          // @ts-ignore
          .flattenDeep()
          .uniq()
          .value();

        const existing_source_dependency_names_text = dedent(`
          SELECT
              array_agg(name) AS existing_source_dependency_names
            FROM data_manager.sources
            WHERE ( name = ANY($1) )
        `);

        existing_source_dependency_names_query = {
          text: existing_source_dependency_names_text,
          values: [all_source_dependency_names],
        };
      }

      stmts.push({
        name,
        exists_query,
        insert_query,
        all_source_dependency_names,
        existing_source_dependency_names_query,
        update_source_dependencies_query,
      });
    }

    return stmts;
  }

  /**
   * Create a tree of DamaSources that involve inter-dependencies.
   *
   * @remarks
   *    DamaSources can depend on other DamaSources.
   *      These dependencies are declared in the data_manager.sources.source_dependencies column.
   *
   *    This method allows declaring a tree of inter-dependent DamaSources as an object,
   *      inserting them into the data_manager.sources table, while auto-populating
   *      the source_dependencies column.
   *
   *    The source_dependencies must be declared using a source_dependencies_names property
   *      because source_ids are not known until after a DamaSource is INSERTed.
   *
   *    NOTE: It is safe to include already existing DamaSources in toposorted_dama_src_meta.
   *
   *    For an example of the toposorted_dama_src_meta datastructure,
   *      see ../../data_types/npmrds/domain toposortedNpmrdsDataSourcesInitialMetadata
   *
   * @param toposorted_dama_sources_meta - An array of DamaSources, toposorted by source_dependencies_names.
   *
   * @param pg_env - The database to connect to. Optional if running in a dama_context EtlContext.
   *
   * @returns an array with the newly inserted DamaSources, in the same order as toposorted_dama_sources_meta
   */
  async loadToposortedDamaSourceMetadata(
    toposorted_dama_sources_meta: Record<string, any>[],
    pg_env = this.pg_env
  ): Promise<Array<DamaSource | null>> {
    // NODE: Since the callback runs in the context, no need for passing pg_env.

    const fn = async () => {
      const queries = await this.generateToposortedLoadDataSourcesQueries(
        toposorted_dama_sources_meta
      );

      const toposorted_dama_src_names: string[] = [];

      for (const {
        name,
        exists_query,
        insert_query,
        all_source_dependency_names,
        existing_source_dependency_names_query,
        update_source_dependencies_query,
      } of queries) {
        toposorted_dama_src_names.push(name);

        const {
          rows: [{ data_source_exists }],
        } = await dama_db.query(exists_query);

        if (data_source_exists) {
          continue;
        }

        await dama_db.query(insert_query);

        if (!all_source_dependency_names) {
          continue;
        }

        const {
          rows: [{ existing_source_dependency_names }],
        } = await dama_db.query(
          <NodePgQueryConfig>existing_source_dependency_names_query
        );

        const missing_srcs = _.difference(
          all_source_dependency_names,
          existing_source_dependency_names
        );

        if (missing_srcs.length) {
          throw new Error(
            `ERROR: The following source_dependencies for ${name} do not exist: ${missing_srcs}`
          );
        }

        await dama_db.query(
          <NodePgQueryConfig>update_source_dependencies_query
        );
      }

      const dama_src_meta_sql = dedent(`
        SELECT
            *
          FROM data_manager.sources
          WHERE ( name = ANY( $1 ) )
      `);

      const { rows: dama_src_meta_rows } = await dama_db.query({
        text: dama_src_meta_sql,
        values: [toposorted_dama_src_names],
      });

      const dama_src_meta_by_name = dama_src_meta_rows.reduce((acc, row) => {
        const { name } = row;
        acc[name] = row;
        return acc;
      }, {});

      const toposorted_dama_src_meta = toposorted_dama_src_names.map(
        (name) => dama_src_meta_by_name[name] || null
      );

      return toposorted_dama_src_meta;
    };

    return dama_db.isInTransactionContext
      ? fn()
      : dama_db.runInTransactionContext(fn, pg_env);
  }

  async getMBTilesMetadataForView(view_id: DamaViewID, pg_env = this.pg_env) {
    const sql = dedent(
      `
        SELECT
            metadata->'dama'->'mbtiles' AS mbtiles_metadata
          FROM data_manager.views
          WHERE ( view_id = $1 )
      `
    );

    const { rows } = await dama_db.query(
      { text: sql, values: [view_id] },
      pg_env
    );

    return rows.length === 0 ? null : rows[0].mbtiles_metadata;
  }

  // Assumption: active_end_timestamp is NULL for authoritative views.
  async getCurrentActiveViewsForDamaSourceName(
    source_name: string,
    pg_env = this.pg_env
  ) {
    // Create the DamaSource if it does not exist.
    const { [source_name]: existing_dama_source } =
      await this.getDamaSourceMetadataByName([source_name], pg_env);

    const source_id = existing_dama_source?.source_id ?? null;

    if (source_id === null) {
      throw new Error(
        `The DamaSource has not been created for ${source_name}.`
      );
    }

    // Get the current authoritative
    const current_authoritative_view_id_sql = dedent(`
      SELECT
          view_id
        FROM data_manager.sources AS a
          INNER JOIN data_manager.views AS b
            USING ( source_id )
        WHERE (
          ( source_id = $1 )
          AND
          ( active_end_timestamp IS NULL )
        )
    `);

    const { rows: authoritative_view_res } = await dama_db.query(
      {
        text: current_authoritative_view_id_sql,
        values: [source_id],
      },
      pg_env
    );

    const view_ids = authoritative_view_res.map(({ view_id }) => view_id);

    return view_ids;
  }
}

export default new DamaMeta();
