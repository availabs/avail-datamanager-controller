CREATE TABLE IF NOT EXISTS _data_manager_admin.etl_context (
  context_id        SERIAL PRIMARY KEY,
  parent_id         INTEGER,

  FOREIGN KEY (parent_id) REFERENCES _data_manager_admin.etl_context(context_id)
) ;

CREATE TABLE IF NOT EXISTS _data_manager_admin.event_store_prototype (
  event_id          SERIAL PRIMARY KEY,
  type              TEXT NOT NULL,
  payload           JSONB,
  meta              JSONB,
  error             BOOLEAN,

  is_checkpoint     BOOLEAN
) ;
