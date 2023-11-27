SELECT
    SUM(
        total_culverts_federal_owned
      + total_culverts_state_owned
      + total_culverts_county_owned
      + total_culverts_municipality_owned
      + total_culverts_auth_or_comm_owned
      + total_culverts_railroad_owned
      + total_culverts_other_owned
    ) AS total_culverts
  FROM nysdot_structures.nysdot_culverts_in_poor_condition_aggregated
;

SELECT
    COUNT(DISTINCT bin) AS total_culverts
  FROM nysdot_structures.culverts
;

SELECT
    SUM(
        total_culverts_poor_condition_federal_owned
      + total_culverts_poor_condition_state_owned
      + total_culverts_poor_condition_county_owned
      + total_culverts_poor_condition_municipality_owned
      + total_culverts_poor_condition_auth_or_comm_owned
      + total_culverts_poor_condition_railroad_owned
      + total_culverts_poor_condition_other_owned
    ) AS total_culverts_poor_condition
  FROM nysdot_structures.nysdot_culverts_in_poor_condition_aggregated
;

SELECT
    COUNT(DISTINCT bin) AS total_culverts_poor_condition
  FROM nysdot_structures.culverts
  WHERE ( condition_r < 5 )
;

SELECT
    SUM(
        xwater_total_culverts_federal_owned
      + xwater_total_culverts_state_owned
      + xwater_total_culverts_county_owned
      + xwater_total_culverts_municipality_owned
      + xwater_total_culverts_auth_or_comm_owned
      + xwater_total_culverts_railroad_owned
      + xwater_total_culverts_other_owned
    ) AS xwater_total_culverts
  FROM nysdot_structures.nysdot_culverts_in_poor_condition_aggregated
;

SELECT
    COUNT(DISTINCT bin) AS xwater_total_culverts
  FROM nysdot_structures.culverts
  WHERE ( stream_bed_m <> '1 - No Waterway' ) 
;

SELECT
    SUM(
        xwater_total_culverts_poor_condition_federal_owned
      + xwater_total_culverts_poor_condition_state_owned
      + xwater_total_culverts_poor_condition_county_owned
      + xwater_total_culverts_poor_condition_municipality_owned
      + xwater_total_culverts_poor_condition_auth_or_comm_owned
      + xwater_total_culverts_poor_condition_railroad_owned
      + xwater_total_culverts_poor_condition_other_owned
    ) AS xwater_total_poor_condition
  FROM nysdot_structures.nysdot_culverts_in_poor_condition_aggregated
;

SELECT
    COUNT(DISTINCT bin) AS xwater_total_poor_condition
  FROM nysdot_structures.culverts
  WHERE (
    ( condition_r < 5 )
    AND
    ( stream_bed_m <> '1 - No Waterway' )
  )
;

