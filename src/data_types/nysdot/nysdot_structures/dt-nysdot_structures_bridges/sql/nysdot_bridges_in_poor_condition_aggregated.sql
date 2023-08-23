DROP VIEW IF EXISTS nysdot_structures.nysdot_bridges_in_poor_condition_aggregated ;
CREATE OR REPLACE VIEW nysdot_structures.nysdot_bridges_in_poor_condition_aggregated AS
  SELECT
      county,

      COALESCE( (summary->'Federal'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_federal_owned,
      COALESCE( (summary->'Federal'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_federal_owned,
      COALESCE( (summary->'Federal'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_federal_owned,

      COALESCE( (summary->'State'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_state_owned,
      COALESCE( (summary->'State'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_state_owned,
      COALESCE( (summary->'State'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_state_owned,
      COALESCE( (summary->'State'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_state_owned,
      COALESCE( (summary->'State'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_state_owned,
      COALESCE( (summary->'State'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_state_owned,

      COALESCE( (summary->'County'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_county_owned,
      COALESCE( (summary->'County'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_county_owned,
      COALESCE( (summary->'County'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_county_owned,
      COALESCE( (summary->'County'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_county_owned,
      COALESCE( (summary->'County'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_county_owned,
      COALESCE( (summary->'County'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_county_owned,

      COALESCE( (summary->'Municipal'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_municipality_owned,
      COALESCE( (summary->'Municipal'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_municipality_owned,
      COALESCE( (summary->'Municipal'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_municipality_owned,

      COALESCE( (summary->'Authority or Commission'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_auth_or_comm_owned,
      COALESCE( (summary->'Authority or Commission'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_auth_or_comm_owned,

      COALESCE( (summary->'Railroad'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_railroad_owned,
      COALESCE( (summary->'Railroad'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_railroad_owned,
      COALESCE( (summary->'Railroad'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_railroad_owned,

      COALESCE( (summary->'Other'->>'total_bridges')::INTEGER, 0 )
        AS total_bridges_other_owned,
      COALESCE( (summary->'Other'->>'total_bridges_poor_condition')::INTEGER, 0 )
        AS total_bridges_poor_condition_other_owned,
      COALESCE( (summary->'Other'->>'xwater_total_bridges')::INTEGER, 0 )
        AS xwater_total_bridges_other_owned,
      COALESCE( (summary->'Other'->>'xwater_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_total_bridges_poor_condition_other_owned,
      COALESCE( (summary->'Other'->>'xwater_unknown_total_bridges')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_other_owned,
      COALESCE( (summary->'Other'->>'xwater_unknown_total_bridges_poor_condition')::INTEGER, 0 )
        AS xwater_unknown_total_bridges_poor_condition_other_owned

    FROM (
      SELECT
          county,

          jsonb_object_agg(
            owner_class,

            breakdown
          ) AS summary

        FROM (
          SELECT
              COALESCE(nysdot_county, usdot_county) AS county,
              COALESCE(nysdot_primary_owner_class, usdot_owner_class) AS owner_class,
              
              jsonb_build_object(

                'total_bridges',
                    COUNT( DISTINCT
                      COALESCE(
                        nysdot_bin,
                        RegExp_Replace(
                          usdot_structure_008,
                          '^0{1,}',
                          ''
                        )
                      )
                    ),

                'total_bridges_poor_condition',         
                    COALESCE(
                      COUNT( DISTINCT
                          COALESCE(
                            nysdot_bin,
                            RegExp_Replace(
                              usdot_structure_008,
                              '^0{1,}',
                              ''
                            )
                          )
                        ) FILTER (
                          WHERE (
                            COALESCE(nysdot_is_in_poor_condition, FALSE)
                            OR 
                            COALESCE(usdot_is_in_poor_condition,  FALSE)
                          )
                        ),
                      0
                    ),

                'xwater_total_bridges',
                    COALESCE(
                      COUNT( DISTINCT
                        COALESCE(
                          nysdot_bin,
                          RegExp_Replace(
                            usdot_structure_008,
                            '^0{1,}',
                            ''
                          )
                        )
                      ) FILTER ( WHERE ( usdot_crosses_water ) ),
                      0
                    ),

                'xwater_total_bridges_poor_condition',
                    COALESCE(
                      COUNT( DISTINCT
                          COALESCE(
                            nysdot_bin,
                            RegExp_Replace(
                              usdot_structure_008,
                              '^0{1,}',
                              ''
                            )
                          )
                        ) FILTER (
                          WHERE (
                            ( usdot_crosses_water )
                            AND
                            (
                              COALESCE(nysdot_is_in_poor_condition, FALSE)
                              OR 
                              COALESCE(usdot_is_in_poor_condition,  FALSE)
                            )
                          )
                        ),
                      0
                    ),

                'xwater_unknown_total_bridges',
                    COALESCE(
                      COUNT( DISTINCT
                          COALESCE(
                            nysdot_bin,
                            RegExp_Replace(
                              usdot_structure_008,
                              '^0{1,}',
                              ''
                            )
                          )
                        ) FILTER ( WHERE ( usdot_crosses_water IS NULL ) ),
                      0
                    ),

                'xwater_unknown_total_bridges_poor_condition',
                    COALESCE(
                      COUNT( DISTINCT
                          COALESCE(
                            nysdot_bin,
                            RegExp_Replace(
                              usdot_structure_008,
                              '^0{1,}',
                              ''
                            )
                          )
                        ) FILTER (
                          WHERE (
                            ( usdot_crosses_water IS NULL )
                            AND
                            (
                              COALESCE(nysdot_is_in_poor_condition, FALSE)
                              OR 
                              COALESCE(usdot_is_in_poor_condition,  FALSE)
                            )
                          )
                        ),
                      0
                    )

            ) AS breakdown

            FROM nysdot_structures.nysdot_usdot_bridges_condition AS t0

            GROUP BY 1,2
        ) AS t1

        GROUP BY 1
  ) AS t2
;
