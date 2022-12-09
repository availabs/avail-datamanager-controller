-- Step 1: Preparation

BEGIN ;

DROP SCHEMA IF EXISTS tmp_dama_migration CASCADE;
CREATE SCHEMA tmp_dama_migration;

CREATE TABLE tmp_dama_migration.sources
  AS
    SELECT
        *
      FROM data_manager.sources
;

CREATE TABLE tmp_dama_migration.views
  AS
    SELECT
        *
      FROM data_manager.views
;

DROP SCHEMA data_manager CASCADE;
DROP SCHEMA _data_manager_admin CASCADE;

COMMIT ;
