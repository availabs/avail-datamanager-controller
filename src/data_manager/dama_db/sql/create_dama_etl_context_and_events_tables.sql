CREATE SCHEMA IF NOT EXISTS data_manager ;

CREATE TABLE IF NOT EXISTS data_manager.etl_statuses (
  etl_status  TEXT PRIMARY KEY
) ;

INSERT INTO data_manager.etl_statuses (
  etl_status
)
  VALUES
    ( 'OPEN' ),
    ( 'DONE' ),
    ( 'ERROR' )
  ON CONFLICT (etl_status) DO NOTHING
;

CREATE TABLE IF NOT EXISTS data_manager.etl_contexts (
  etl_context_id            SERIAL PRIMARY KEY,
  parent_context_id         INTEGER,

  source_id                 INTEGER,

  etl_task_id               TEXT,
  etl_status                TEXT,

  initial_event_id          INTEGER,
  latest_event_id           INTEGER,

  _created_timestamp        TIMESTAMP NOT NULL DEFAULT NOW(),
  _modified_timestamp       TIMESTAMP NOT NULL DEFAULT NOW(),

  FOREIGN KEY (parent_context_id)
    REFERENCES data_manager.etl_contexts(etl_context_id)
    ON DELETE CASCADE,

  -- -- This FKey appears to cause major locking issues.
  -- FOREIGN KEY (source_id)
  --   REFERENCES data_manager.sources(source_id)
  --   ON DELETE CASCADE,

  FOREIGN KEY (etl_status)
    REFERENCES data_manager.etl_statuses(etl_status)
    ON DELETE CASCADE
) ;

-- NOTE: This causes the TaskRunner to lock up.
-- ALTER TABLE data_manager.etl_contexts
--   ADD COLUMN IF NOT EXISTS etl_task_id TEXT
-- ;

CREATE TABLE IF NOT EXISTS data_manager.event_store (
  event_id                  SERIAL PRIMARY KEY,
  etl_context_id                INTEGER NOT NULL,

  type                      TEXT NOT NULL,
  payload                   JSONB,
  meta                      JSONB,
  error                     BOOLEAN,

  _created_timestamp        TIMESTAMP NOT NULL DEFAULT NOW(),

  FOREIGN KEY (etl_context_id)
    REFERENCES data_manager.etl_contexts(etl_context_id)
    ON DELETE CASCADE
) ;

CREATE INDEX IF NOT EXISTS event_store_etl_ctx_idx
  ON data_manager.event_store (etl_context_id)
;

CREATE OR REPLACE FUNCTION data_manager.event_store_etl_context_status_update_fn()
  RETURNS TRIGGER
  LANGUAGE plpgsql
  AS
  $$
    DECLARE
      initial_exists_for_ctx  BOOLEAN ;
      ctx_is_done  BOOLEAN ;

    BEGIN
      SELECT
          initial_event_id IS NOT NULL,
          etl_status = 'DONE'
        FROM data_manager.etl_contexts
        WHERE ( etl_context_id = NEW.etl_context_id )
        INTO initial_exists_for_ctx, ctx_is_done
      ;

      -- Done is Done. event_store already has a :FINAL event for this etl_context.
      IF (ctx_is_done)
        THEN
          RAISE EXCEPTION
            'ETL context %. is DONE. It cannot receive any more events.',
            NEW.etl_context_id
          ;

      END IF ;

      IF ( NEW.type LIKE '%:INITIAL' )
        THEN

          -- There MUST be ONLY one INITIAL event.
          IF ( initial_exists_for_ctx )
            THEN
              RAISE
                EXCEPTION ':INITIAL event already exists for ETL context %.',
                NEW.etl_context_id
              ;
          END IF ;

          UPDATE data_manager.etl_contexts
            SET etl_status        = 'OPEN',
                initial_event_id  = NEW.event_id,
                latest_event_id   = NEW.event_id
            WHERE ( etl_context_id = NEW.etl_context_id )
          ;

          RETURN NULL ;

        -- All EtlContexts MUST begin with an INITIAL event.
        ELSIF ( NOT initial_exists_for_ctx )
          THEN
              RAISE
                EXCEPTION 'All ETL Contexts MUST begin with an :INITIAL event.'
              ;

      END IF;

      IF ( NEW.type LIKE '%:FINAL' )
        THEN
          UPDATE data_manager.etl_contexts
            SET etl_status        = 'DONE',
                latest_event_id   = NEW.event_id
            WHERE ( etl_context_id = NEW.etl_context_id )
          ;

          PERFORM pg_notify(
            'ETL_CONTEXT_FINAL_EVENT',
            NEW.etl_context_id::TEXT
          ) ;

          RETURN NULL ;
      END IF ;

      IF ( NEW.type LIKE '%:ERROR' )
        THEN
          UPDATE data_manager.etl_contexts
            SET etl_status        = 'ERROR',
                latest_event_id   = NEW.event_id
            WHERE ( etl_context_id = NEW.etl_context_id )
          ;

          RETURN NULL ;
      END IF ;

      -- NOTE: This MAY switch ERROR to OPEN for retries.
      UPDATE data_manager.etl_contexts
        SET etl_status        = 'OPEN',
            latest_event_id   = NEW.event_id
        WHERE ( etl_context_id = NEW.etl_context_id )
      ;

      RETURN NULL ;

    END ;
  $$
;

DO
  LANGUAGE plpgsql
  $$
    DECLARE
      it_exists  BOOLEAN ;

    BEGIN

      SELECT EXISTS (
        SELECT
            1
          FROM information_schema.triggers
          WHERE (
            ( trigger_schema = 'data_manager' )
            AND
            ( trigger_name = 'etl_contexts_modified_timestamp_trigger' )
          )
      ) INTO it_exists ;

      IF NOT it_exists
        THEN
          CREATE TRIGGER etl_contexts_modified_timestamp_trigger
            BEFORE UPDATE
            ON data_manager.etl_contexts
            FOR EACH ROW
            EXECUTE PROCEDURE data_manager.dama_update_modified_timestamp_fn();
      END IF ;

      SELECT EXISTS (
        SELECT
            1
          FROM information_schema.triggers
          WHERE (
            ( trigger_schema = 'data_manager' )
            AND
            ( trigger_name = 'event_store_etl_context_status_update_trigger' )
          )
      ) INTO it_exists ;

      IF NOT it_exists
        THEN
          CREATE TRIGGER event_store_etl_context_status_update_trigger
            AFTER INSERT
            ON data_manager.event_store
            FOR EACH ROW
            EXECUTE PROCEDURE data_manager.event_store_etl_context_status_update_fn();
      END IF ;


      SELECT EXISTS (

        -- https://dba.stackexchange.com/a/214877
        SELECT 1
          FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
              ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
              ON nsp.oid = connamespace
          WHERE (
            ( con.conname = 'views_etl_ctx_id_fkey' )
            AND
            ( nsp.nspname = 'data_manager' )
            AND
            ( rel.relname = 'views' )
          )

      ) INTO it_exists ;

      IF NOT it_exists
        THEN
          ALTER TABLE data_manager.views
            ADD CONSTRAINT views_etl_ctx_id_fkey
            FOREIGN KEY (etl_context_id)
              REFERENCES data_manager.etl_contexts (etl_context_id)
              -- We do _NOT_ want "ON DELETE CASCADE" here
              -- We do want to prevent etl_contexts DELETEs.
          ;
      END IF ;

      SELECT EXISTS (

        -- https://dba.stackexchange.com/a/214877
        SELECT 1
          FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
              ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
              ON nsp.oid = connamespace
          WHERE (
            ( con.conname = 'etl_contexts_initial_event_id_fkey' )
            AND
            ( nsp.nspname = 'data_manager' )
            AND
            ( rel.relname = 'etl_contexts' )
          )

      ) INTO it_exists ;

      IF NOT it_exists
        THEN
          ALTER TABLE data_manager.etl_contexts
            ADD CONSTRAINT etl_contexts_initial_event_id_fkey
            FOREIGN KEY (initial_event_id)
              REFERENCES data_manager.event_store (event_id)
              ON DELETE CASCADE
          ;
      END IF ;

      SELECT EXISTS (

        -- https://dba.stackexchange.com/a/214877
        SELECT 1
          FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
              ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
              ON nsp.oid = connamespace
          WHERE (
            ( con.conname = 'etl_contexts_latest_event_id_fkey' )
            AND
            ( nsp.nspname = 'data_manager' )
            AND
            ( rel.relname = 'etl_contexts' )
          )

      ) INTO it_exists ;

      IF NOT it_exists
        THEN
          ALTER TABLE data_manager.etl_contexts
            ADD CONSTRAINT etl_contexts_latest_event_id_fkey
            FOREIGN KEY (latest_event_id)
              REFERENCES data_manager.event_store (event_id)
              ON DELETE CASCADE
          ;
      END IF ;

    END
  $$
;

CREATE OR REPLACE FUNCTION data_manager.show_etl_context_tree(root_context_id INTEGER)
  RETURNS TABLE (
    etl_context_id      INTEGER,
    parent_context_id   INTEGER,
    source_id           INTEGER,
    etl_status          TEXT,
    initial_event_id    INTEGER,
    latest_event_id     INTEGER,
    note                TEXT,
    _created_timestamp  TIMESTAMP,
    _modified_timestamp TIMESTAMP
  )
  AS
  $$
      WITH RECURSIVE cte_ctx_tree(etl_context_id, parent_context_id) AS (
        SELECT
            etl_context_id,
            parent_context_id
          FROM data_manager.etl_contexts
          WHERE etl_context_id = $1
        UNION
        SELECT
            a.etl_context_id,
            a.parent_context_id
          FROM data_manager.etl_contexts AS a
            INNER JOIN cte_ctx_tree
              ON (
                ( a.etl_context_id = cte_ctx_tree.parent_context_id )
                OR
                ( a.parent_context_id = cte_ctx_tree.etl_context_id )
              )
      )
        SELECT
            a.etl_context_id,
            a.parent_context_id,
            a.source_id,
            a.etl_status,
            a.initial_event_id,
            a.latest_event_id,
            c.meta->>'note' AS note,
            a._created_timestamp,
            a._modified_timestamp
          FROM data_manager.etl_contexts AS a
            INNER JOIN cte_ctx_tree AS b
              USING (etl_context_id)
            LEFT OUTER JOIN data_manager.event_store AS c
              ON ( a.initial_event_id = c.event_id )
          ORDER BY 1
      ;
  $$
  LANGUAGE SQL
;
