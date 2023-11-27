/*
dama_dev_1=# select count(1) from culverts where county is null;
 count 
-------
     0
(1 row)

dama_dev_1=# select count(1) from culverts where primary_own is null;
primary_own
dama_dev_1=# select count(1) from culverts where primary_own is null;
 count 
-------
     0
(1 row)


*/

DROP VIEW IF EXISTS nysdot_structures.nysdot_culverts_in_poor_condition_aggregated ;
CREATE OR REPLACE VIEW nysdot_structures.nysdot_culverts_in_poor_condition_aggregated AS
  SELECT
      county,

      COALESCE( (summary->'Federal'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_federal_owned,
      COALESCE( (summary->'Federal'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_federal_owned,
      COALESCE( (summary->'Federal'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_total_culverts_condition_unk_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_unk_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_unk_total_culverts_poor_condition_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_unk_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_unk_total_culverts_condition_unk_federal_owned,

      COALESCE( (summary->'State'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_state_owned,
      COALESCE( (summary->'State'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_state_owned,
      COALESCE( (summary->'State'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_state_owned,
      COALESCE( (summary->'State'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_state_owned,
      COALESCE( (summary->'State'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_state_owned,
      COALESCE( (summary->'State'->>'xwater_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_total_culverts_condition_unk_state_owned,
      COALESCE( (summary->'State'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_state_owned,
      COALESCE( (summary->'State'->>'xwater_unk_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_unk_total_culverts_poor_condition_state_owned,
      COALESCE( (summary->'State'->>'xwater_unk_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_unk_total_culverts_condition_unk_state_owned,

      COALESCE( (summary->'County'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_county_owned,
      COALESCE( (summary->'County'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_county_owned,
      COALESCE( (summary->'County'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_county_owned,
      COALESCE( (summary->'County'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_county_owned,
      COALESCE( (summary->'County'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_county_owned,
      COALESCE( (summary->'County'->>'xwater_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_total_culverts_condition_unk_county_owned,
      COALESCE( (summary->'County'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_county_owned,
      COALESCE( (summary->'County'->>'xwater_unk_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_unk_total_culverts_poor_condition_county_owned,
      COALESCE( (summary->'County'->>'xwater_unk_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_unk_total_culverts_condition_unk_county_owned,

      COALESCE( (summary->'Municipal'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_municipality_owned,
      COALESCE( (summary->'Municipal'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_municipality_owned,
      COALESCE( (summary->'Municipal'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_total_culverts_condition_unk_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_unk_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_unk_total_culverts_poor_condition_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_unk_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_unk_total_culverts_condition_unk_municipality_owned,

      COALESCE( (summary->'Authority or Commission'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_auth_or_comm_owned,
      COALESCE(
        (summary->'Authority or Commission'->>'xwater_total_culverts_condition_unk_auth_or_comm')::INTEGER,
        0
      ) AS xwater_total_culverts_condition_unk_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_auth_or_comm_owned,
      COALESCE(
        (summary->'Authority or Commission'->>'xwater_unk_total_culverts_poor_condition')::INTEGER,
        0 
      ) AS xwater_unk_total_culverts_poor_condition_auth_or_comm_owned,
      COALESCE(
        (summary->'Authority or Commission'->>'xwater_unk_total_culverts_condition_unk')::INTEGER,
        0 
      ) AS xwater_unk_total_culverts_condition_unk_auth_or_comm_owned,

      COALESCE( (summary->'Railroad'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_railroad_owned,
      COALESCE( (summary->'Railroad'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_railroad_owned,
      COALESCE( (summary->'Railroad'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_total_culverts_condition_unk_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_unk_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_unk_total_culverts_poor_condition_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_unk_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_unk_total_culverts_condition_unk_railroad_owned,

      COALESCE( (summary->'Other'->>'total_culverts')::INTEGER, 0 )
        AS total_culverts_other_owned,
      COALESCE( (summary->'Other'->>'total_culverts_poor_condition')::INTEGER, 0 )
        AS total_culverts_poor_condition_other_owned,
      COALESCE( (summary->'Other'->>'total_culverts_condition_unk')::INTEGER, 0 )
        AS total_culverts_condition_unk_other_owned,
      COALESCE( (summary->'Other'->>'xwater_total_culverts')::INTEGER, 0 )
        AS xwater_total_culverts_other_owned,
      COALESCE( (summary->'Other'->>'xwater_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_total_culverts_poor_condition_other_owned,
      COALESCE( (summary->'Other'->>'xwater_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_total_culverts_condition_unk_other_owned,
      COALESCE( (summary->'Other'->>'xwater_unk_total_culverts')::INTEGER, 0 )
        AS xwater_unk_total_culverts_other_owned,
      COALESCE( (summary->'Other'->>'xwater_unk_total_culverts_poor_condition')::INTEGER, 0 )
        AS xwater_unk_total_culverts_poor_condition_other_owned,
      COALESCE( (summary->'Other'->>'xwater_unk_total_culverts_condition_unk')::INTEGER, 0 )
        AS xwater_unk_total_culverts_condition_unk_other_owned

    FROM (
      SELECT
          county,

          jsonb_object_agg(
            owner_class,

            breakdown
          ) AS summary

        FROM (
          SELECT
              nysdot_county AS county,
              nysdot_primary_owner_class AS owner_class,
              
              jsonb_build_object(

                'total_culverts',
                    COUNT( DISTINCT nysdot_bin ),

                'total_culverts_poor_condition',         
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER (
                          WHERE ( nysdot_is_in_poor_condition )
                        ),
                      0
                    ),

                'total_culverts_condition_unk',         
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER (
                          WHERE ( nysdot_is_in_poor_condition IS NULL )
                        ),
                      0
                    ),

                'xwater_total_culverts',
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER ( WHERE ( nysdot_crosses_water ) ),
                      0
                    ),

                'xwater_total_culverts_poor_condition',
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER (
                          WHERE (
                            ( nysdot_crosses_water )
                            AND
                            ( nysdot_is_in_poor_condition )
                          )
                        ),
                      0
                    ),

                'xwater_total_culverts_condition_unk',
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER (
                          WHERE (
                            ( nysdot_crosses_water )
                            AND
                            ( nysdot_is_in_poor_condition IS NULL )
                          )
                        ),
                      0
                    ),

                'xwater_unk_total_culverts',
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER ( WHERE ( nysdot_crosses_water IS NULL ) ),
                      0
                    ),

                'xwater_unk_total_culverts_poor_condition',
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER (
                          WHERE (
                            ( nysdot_crosses_water IS NULL )
                            AND
                            ( nysdot_is_in_poor_condition )
                          )
                        ),
                      0
                    ),

                'xwater_unk_total_culverts_condition_unk',
                    COALESCE(
                      COUNT( DISTINCT nysdot_bin )
                        FILTER (
                          WHERE (
                            ( nysdot_crosses_water IS NULL )
                            AND
                            ( nysdot_is_in_poor_condition IS NULL )
                          )
                        ),
                      0
                    )

            ) AS breakdown

            FROM nysdot_structures.nysdot_culverts_condition AS t0

            GROUP BY 1,2
        ) AS t1

        GROUP BY 1
  ) AS t2
;
