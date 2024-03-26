BEGIN ;

-- Create the QA TABLEs and VIEWs.

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.qa_double_counted_parcels CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.qa_double_counted_parcels (
  ogc_fid         INTEGER,
  p_ogc_fid       INTEGER,
  num_occurances  INTEGER CHECK (num_occurances > 1),

  PRIMARY KEY (ogc_fid, p_ogc_fid)
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.qa_double_counted_parcels (
  ogc_fid,
  p_ogc_fid,
  num_occurances
)
  SELECT
      ogc_fid,
      p_ogc_fid,
      COUNT(1) AS num_occurances
    FROM (
      SELECT
          ogc_fid,
          UNNEST(p_ogc_fid_arr) AS p_ogc_fid
        FROM :OUTPUT_SCHEMA.lcgu_bldg_poly_value
    ) AS t
    GROUP BY ogc_fid, p_ogc_fid
    HAVING ( COUNT(1) > 1 )
;

CREATE VIEW :OUTPUT_SCHEMA.qa_no_parcel_double_counting_per_building_test
  AS
    SELECT NOT EXISTS (
      SELECT
          1
        FROM :OUTPUT_SCHEMA.qa_double_counted_parcels
    ) AS passed
;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.qa_total_valuation_equality CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.qa_total_valuation_equality (
  parcels_total_nonland_av        MONEY NOT NULL,
  buildings_total_av              MONEY NOT NULL,
  total_nonland_av_difference     MONEY NOT NULL,
  difference_ratio                FLOAT NOT NULL
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.qa_total_valuation_equality (
  parcels_total_nonland_av,
  buildings_total_av,
  total_nonland_av_difference,
  difference_ratio
)
  SELECT
      CAST(a.parcels_total_nonland_av AS MONEY),
      CAST(b.buildings_total_av AS MONEY),
      CAST(buildings_total_av - parcels_total_nonland_av AS MONEY),
      ROUND(
        (buildings_total_av - parcels_total_nonland_av)
        / parcels_total_nonland_av,
        5
      )::FLOAT AS difference_ratio
    FROM (
      -- Total non-land assessed value for parcels that intersect at least one building.
      SELECT
          SUM( p_nonland_av )::NUMERIC AS parcels_total_nonland_av
        FROM (
          SELECT DISTINCT ON (p_ogc_fid)
              p_ogc_fid,
              GREATEST( p_total_av - p_land_av ) AS p_nonland_av
            FROM :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties
            WHERE ( n_ogc_fid IS NOT NULL )
            ORDER BY p_ogc_fid
        )
    ) AS a CROSS JOIN (
      SELECT
          SUM( p_nonland_av )::NUMERIC AS buildings_total_av
        FROM :OUTPUT_SCHEMA.lcgu_bldg_poly_value
    ) AS b
;

CREATE VIEW :OUTPUT_SCHEMA.qa_total_valuation_equality_test
  AS
    SELECT NOT EXISTS (
      SELECT
          1
        FROM :OUTPUT_SCHEMA.qa_double_counted_parcels
    ) AS passed -- Three 9's precision
;

-- We commit because we want to preserve the failing cases for inspection/debugging.
COMMIT ;

-- Do the QA
BEGIN ;

\set FAILING_TESTS ''

SELECT
    string_agg(failed_test, ', ') AS "QA_FAILING_TESTS",
    (COUNT(1) > 0) AS "QA_TESTS_FAILED"
  FROM (
    SELECT
        'QA_NO_PARCEL_VALUATION_DOUBLE_COUNTING' AS failed_test
      FROM :OUTPUT_SCHEMA.qa_no_parcel_double_counting_per_building_test
      WHERE ( NOT passed )
    UNION ALL
    SELECT
        'QA_TOTAL_VALUATION_EQUALITY' AS failed_test
      FROM :OUTPUT_SCHEMA.qa_total_valuation_equality_test
      WHERE ( NOT passed )
  ) AS t
\gset

\if :QA_TESTS_FAILED
  CREATE TEMPORARY TABLE tmp_failing_tests
    ON COMMIT DROP
    AS SELECT :'QA_FAILING_TESTS' AS failing_tests
  ;

  DO $$
    BEGIN
      RAISE EXCEPTION 'The following QA tests failed %s', (SELECT failing_tests FROM tmp_failing_tests) ;
  END $$ ;

\endif

COMMIT ;
