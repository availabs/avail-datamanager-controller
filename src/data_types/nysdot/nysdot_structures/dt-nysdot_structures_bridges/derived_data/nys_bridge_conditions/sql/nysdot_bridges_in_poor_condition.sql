COPY (
  SELECT
      nysdot_bin,
      usdot_structure_008,
      nysdot_county,
      nysdot_primary_owner_class,
      nysdot_primary_owner,
      nysdot_primary_maintenance,
      nysdot_condition_rating,
      nysdot_crossed,
      ROUND(nysdot_longitude::NUMERIC, 6) AS nysdot_longitude,
      ROUND(nysdot_latitude::NUMERIC, 6) AS nysdot_latitude,
      nysdot_is_in_poor_condition,

      usdot_location_009,

      usdot_adt_029,
      usdot_year_adt_030,
      usdot_detour_km_019,

      usdot_maintainer_class,
      usdot_maintainer_description,
      usdot_owner_class,
      usdot_owner_description,
      usdot_deck_condition_058,
      usdot_superstructure_condition_059,
      usdot_substructure_condition_060,
      usdot_channel_condition_061,
      usdot_culvert_condition_062,
      usdot_type_of_service_under_bridge_code_042b,
      usdot_type_of_service_under_bridge_description,
      usdot_features_desc_006a,
      usdot_crosses_water,
      ROUND(usdot_longitude::NUMERIC, 6) AS usdot_longitude,
      ROUND(usdot_latitude::NUMERIC, 6) AS usdot_latitude,
      usdot_is_in_poor_condition,

      ROUND(nysdot_usdot_location_difference_meters::NUMERIC, 3) AS nysdot_usdot_location_difference_meters

    FROM nysdot_structures.nysdot_usdot_bridges_condition
    WHERE ( nysdot_is_in_poor_condition OR usdot_is_in_poor_condition )
) TO STDOUT WITH CSV HEADER ;
