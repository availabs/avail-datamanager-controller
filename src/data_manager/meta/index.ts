import _ from "lodash";

import dedent from "dedent";
import pgFormat from "pg-format";

import DamaContextAttachedResource from "data_manager/contexts";

import dama_db from "data_manager/dama_db";
import logger from "data_manager/logger";

import { NodePgQueryConfig } from "../dama_db/postgres/PostgreSQL";

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
      console.log(JSON.stringify({ text, values }, null, 4));
      const table = pgFormat("%I.%I", table_schema, table_name);

      throw new Error(`No such table ${table}`);
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

  async getDamaViewTableColumns(
    dama_view_id: number,
    pg_env = this.pg_env
  ): Promise<string[]> {
    const { table_schema, table_name } =
      await this.getDamaViewTableSchemaAndName(dama_view_id, pg_env);

    return this.getTableColumns(table_schema, table_name, pg_env);
  }

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

  // TODO:  Add a strict mode that throws if row schema does not match table schema.
  //        Could use JSON-Schema to validate/coerce.
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

        const v = row[col];
        acc.values.push(v === "" ? null : v);

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

  async getInsertDamaViewRowQuery(
    view_meta: Record<string, any>,
    pg_env = this.pg_env
  ) {
    const column_names: string[] = await this.getTableColumns(
      "data_manager",
      "views",
      pg_env
    );

    const new_row = _.pick(view_meta, column_names);

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
    const { data_source_name } = view_meta;
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

  async getDamaViewName(dama_view_id: number, pg_env = this.pg_env) {
    const { dama_view_name } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_view_name",
      pg_env
    );

    return dama_view_name;
  }

  async getDamaViewNamePrefix(dama_view_id: number, pg_env = this.pg_env) {
    const { dama_view_name_prefix } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_view_name_prefix",
      pg_env
    );

    return dama_view_name_prefix;
  }

  async getDamaViewGlobalId(dama_view_id: number, pg_env = this.pg_env) {
    const { dama_global_id } = await this.getDamaViewProperties(
      dama_view_id,
      "dama_global_id",
      pg_env
    );

    return dama_global_id;
  }

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

  async generateCreateDamaSourceQuery(
    table_schema: string,
    table_name: string,
    new_row: Record<string, any>,
    pg_env = this.pg_env
  ) {
    return this.generateInsertStatement(
      table_schema,
      table_name,
      new_row,
      pg_env
    );
  }

  async createNewDamaSource(
    new_row: Record<string, any>,
    pg_env = this.pg_env
  ) {
    const q = await this.generateCreateDamaSourceQuery(
      "data_manager",
      "sources",
      new_row,
      pg_env
    );

    const {
      rows: [new_dama_source],
    } = await dama_db.query(q, pg_env);

    logger.info(
      `dama_meta: Created new DamaSource ${new_dama_source.source_id}`
    );
    logger.silly(`dama_meta: ${JSON.stringify(new_dama_source, null, 4)}`);

    return new_dama_source;
  }

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

  async createNewDamaView(new_row: Record<string, any>, pg_env = this.pg_env) {
    const q = await this.generateCreateDamaViewSql(new_row, pg_env);

    const {
      rows: [new_dama_view],
    } = await dama_db.query(q, pg_env);

    logger.info(`dama_meta: Created new DamaView ${new_dama_view.view_id}`);
    logger.silly(`dama_meta: ${JSON.stringify(new_dama_view, null, 4)}`);

    return new_dama_view;
  }

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

  async deleteDamaView(dama_view_id: number, pg_env = this.pg_env) {
    const sql = `DELETE FROM data_manager.views where view_id = ${dama_view_id}`;

    const res = await dama_db.query(sql, pg_env);

    logger.info("dama_meta deleteDamaView", res);

    return sql;
  }

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

  async getDamaSourceMetadataByName(
    dama_source_names: string[],
    pg_env = this.pg_env
  ) {
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

    // @ts-ignore
    const metaByName = dama_source_names.reduce(
      (acc: Record<string, any>, name: string) => {
        acc[name] = rowsByName[name] || null;

        return acc;
      },
      {}
    );

    return metaByName;
  }

  async generateToposortedLoadDataSourcesQueries(
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

  async loadToposortedDamaSourceMetadata(
    toposorted_dama_sources_meta: Record<string, any>[],
    pg_env = this.pg_env
  ) {
    // NODE: Since the callback runs in the context, no need for passing pg_env.
    return dama_db.runInTransactionContext(async () => {
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

      const dama_src_meta_values = [toposorted_dama_src_names];

      const { rows: dama_src_meta_rows } = await dama_db.query({
        text: dama_src_meta_sql,
        values: dama_src_meta_values,
      });

      const dama_src_meta_by_name = dama_src_meta_rows.reduce((acc, row) => {
        const { name } = row;
        acc[name] = row;
        return acc;
      }, {});

      const toposorted_dama_src_meta = toposorted_dama_src_names.map(
        (name) => dama_src_meta_by_name[name]
      );

      return toposorted_dama_src_meta;
    }, pg_env);
  }
}

export default new DamaMeta();
