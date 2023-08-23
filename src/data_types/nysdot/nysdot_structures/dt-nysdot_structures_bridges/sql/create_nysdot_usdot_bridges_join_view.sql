/*
dama_dev_1=# select nysdot_bin, distance_meters, nysdot_location_la, nysdot_last_inspec from nysdot_usdot_bridges_join where nysdot_bin in (select bin from bridges group by 1 having count(1) > 1) order by 1,2,3,4; nysdot_bin | distance_meters | nysdot_location_la | nysdot_last_inspec 
------------+-----------------+--------------------+--------------------
 1025040    |    578.51274095 | 2020-03-04         | 2023-04-25
 1025040    |    578.51534685 | 2020-03-04         | 2023-04-25
 1091459    |     31.03681556 |                    | 2022-05-17
 1091459    |    515.02708621 | 2020-07-02         | 2022-05-17
 3369100    |      0.48359057 |                    | 2022-05-17
 3369100    | 321125.53489966 | 2021-03-17         | 2022-05-17
 5524890    |       1.1256802 | 2021-12-06         | 2022-08-09
 5524890    |   4842.16670887 | 2010-12-08         | 2022-08-09
 5524900    |      1.02156578 | 2021-12-06         | 2022-08-09
 5524900    |   4230.46551531 | 2009-06-10         | 2022-08-09
(10 rows)
*/


BEGIN ;

DROP VIEW IF EXISTS nysdot_structures.nysdot_usdot_bridges_join CASCADE;

CREATE VIEW nysdot_structures.nysdot_usdot_bridges_join
  AS
      SELECT DISTINCT ON (nysdot_bin, usdot_structure_008)
          a.ogc_fid       AS  nysdot_ogc_fid,
          a.object_id     AS  nysdot_object_id,
          a.bin           AS  nysdot_bin,
          a.location_la   AS  nysdot_location_la,
          a.carried       AS  nysdot_carried,
          a.crossed       AS  nysdot_crossed,
          a.primary_own   AS  nysdot_primary_own,
          a.primary_mai   AS  nysdot_primary_mai,
          a.county        AS  nysdot_county,
          a.region        AS  nysdot_region,
          a.gtms_struct   AS  nysdot_gtms_struct,
          a.gtms_materi   AS  nysdot_gtms_materi,
          a.number_of_sp  AS  nysdot_number_of_sp,
          a.condition_r   AS  nysdot_condition_r,
          a.last_inspec   AS  nysdot_last_inspec,
          a.bridge_leng   AS  nysdot_bridge_leng,
          a.deck_area_sq  AS  nysdot_deck_area_sq,
          a.aadt          AS  nysdot_aadt,
          a.year_built    AS  nysdot_year_built,
          a.posted_load   AS  nysdot_posted_load,
          a.r_posted      AS  nysdot_r_posted,
          a.other         AS  nysdot_other,
          a.redc          AS  nysdot_redc,
          a.nbi_deck_co   AS  nysdot_nbi_deck_co,
          a.nbi_substr    AS  nysdot_nbi_substr,
          a.nbi_supers    AS  nysdot_nbi_supers,
          a.fhwa_condi    AS  nysdot_fhwa_condi,

          b.ogc_fid       AS  usdot_ogc_fid,
          b.objectid      AS  usdot_objectid,
          b.state_code    AS  usdot_state_code,
          b.structure     AS  usdot_structure_008,
          b.record_typ    AS  usdot_record_typ,
          b.route_pref    AS  usdot_route_pref,
          b.service_le    AS  usdot_service_le,
          b.route_numb    AS  usdot_route_numb,
          b.direction     AS  usdot_direction,
          b.highway_di    AS  usdot_highway_di,
          b.county_cod    AS  usdot_county_cod,
          b.place_code    AS  usdot_place_code,
          b.features_d    AS  usdot_features_d,
          b.critical_f    AS  usdot_critical_f,
          b.facility_c    AS  usdot_facility_c,
          b.location_0    AS  usdot_location_0,
          b.min_vert_c    AS  usdot_min_vert_c,
          b.kilopoint     AS  usdot_kilopoint,
          b.base_hwy_n    AS  usdot_base_hwy_n,
          b.lrs_inv_ro    AS  usdot_lrs_inv_ro,
          b.subroute_n    AS  usdot_subroute_n,
          b.lat_016       AS  usdot_lat_016,
          b.long_017      AS  usdot_long_017,
          b.detour_kil    AS  usdot_detour_kil,
          b.toll_020      AS  usdot_toll_020,
          b.maintenanc    AS  usdot_maintenanc,
          b.owner_022     AS  usdot_owner_022,
          b.functional    AS  usdot_functional,
          b.year_built    AS  usdot_year_built,
          b.traffic_la    AS  usdot_traffic_la,
          b.traffic_1     AS  usdot_traffic_1,
          b.adt_029       AS  usdot_adt_029,
          b.year_adt_0    AS  usdot_year_adt_0,
          b.design_loa    AS  usdot_design_loa,
          b.appr_width    AS  usdot_appr_width,
          b.median_cod    AS  usdot_median_cod,
          b.degrees_sk    AS  usdot_degrees_sk,
          b.structur_1    AS  usdot_structur_1,
          b.railings_0    AS  usdot_railings_0,
          b.transition    AS  usdot_transition,
          b.appr_rail     AS  usdot_appr_rail,
          b.appr_rai_1    AS  usdot_appr_rai_1,
          b.history_03    AS  usdot_history_03,
          b.navigation    AS  usdot_navigation,
          b.nav_vert_c    AS  usdot_nav_vert_c,
          b.nav_horr_c    AS  usdot_nav_horr_c,
          b.open_close    AS  usdot_open_close,
          b.service_on    AS  usdot_service_on,
          b.service_un    AS  usdot_service_un,
          b.structur_2    AS  usdot_structur_2,
          b.structur_3    AS  usdot_structur_3,
          b.appr_kind     AS  usdot_appr_kind,
          b.appr_type     AS  usdot_appr_type,
          b.main_unit     AS  usdot_main_unit,
          b.appr_spans    AS  usdot_appr_spans,
          b.horr_clr_m    AS  usdot_horr_clr_m,
          b.max_span_l    AS  usdot_max_span_l,
          b.structur_4    AS  usdot_structur_4,
          b.left_curb     AS  usdot_left_curb,
          b.right_curb    AS  usdot_right_curb,
          b.roadway_wi    AS  usdot_roadway_wi,
          b.deck_width    AS  usdot_deck_width,
          b.vert_clr_o    AS  usdot_vert_clr_o,
          b.vert_clr_u    AS  usdot_vert_clr_u,
          b.vert_clr_1    AS  usdot_vert_clr_1,
          b.lat_und_re    AS  usdot_lat_und_re,
          b.lat_und_mt    AS  usdot_lat_und_mt,
          b.left_lat_u    AS  usdot_left_lat_u,
          b.deck_cond     AS  usdot_deck_cond,
          b.superstruc    AS  usdot_superstruc,
          b.substructu    AS  usdot_substructu,
          b.channel_co    AS  usdot_channel_co,
          b.culvert_co    AS  usdot_culvert_co,
          b.opr_rating    AS  usdot_opr_rating,
          b.operating     AS  usdot_operating,
          b.inv_rating    AS  usdot_inv_rating,
          b.inventory     AS  usdot_inventory,
          b.structural    AS  usdot_structural,
          b.deck_geome    AS  usdot_deck_geome,
          b.undclrence    AS  usdot_undclrence,
          b.posting_ev    AS  usdot_posting_ev,
          b.waterway_e    AS  usdot_waterway_e,
          b.appr_road     AS  usdot_appr_road,
          b.work_propo    AS  usdot_work_propo,
          b.work_done     AS  usdot_work_done,
          b.imp_len_mt    AS  usdot_imp_len_mt,
          b.date_of_in    AS  usdot_date_of_in,
          b.inspect_fr    AS  usdot_inspect_fr,
          b.fracture_0    AS  usdot_fracture_0,
          b.undwater_l    AS  usdot_undwater_l,
          b.spec_inspe    AS  usdot_spec_inspe,
          b.fracture_l    AS  usdot_fracture_l,
          b.undwater_1    AS  usdot_undwater_1,
          b.spec_last     AS  usdot_spec_last,
          b.bridge_imp    AS  usdot_bridge_imp,
          b.roadway_im    AS  usdot_roadway_im,
          b.total_imp     AS  usdot_total_imp,
          b.year_of_im    AS  usdot_year_of_im,
          b.other_stat    AS  usdot_other_stat,
          b.other_st_1    AS  usdot_other_st_1,
          b.othr_state    AS  usdot_othr_state,
          b.strahnet_h    AS  usdot_strahnet_h,
          b.parallel_s    AS  usdot_parallel_s,
          b.traffic_di    AS  usdot_traffic_di,
          b.temp_struc    AS  usdot_temp_struc,
          b.highway_sy    AS  usdot_highway_sy,
          b.federal_la    AS  usdot_federal_la,
          b.year_recon    AS  usdot_year_recon,
          b.deck_struc    AS  usdot_deck_struc,
          b.surface_ty    AS  usdot_surface_ty,
          b.membrane_t    AS  usdot_membrane_t,
          b.deck_prote    AS  usdot_deck_prote,
          b.percent_ad    AS  usdot_percent_ad,
          b.national_n    AS  usdot_national_n,
          b.pier_prote    AS  usdot_pier_prote,
          b.bridge_len    AS  usdot_bridge_len,
          b.scour_crit    AS  usdot_scour_crit,
          b.future_adt    AS  usdot_future_adt,
          b.year_of_fu    AS  usdot_year_of_fu,
          b.min_nav_cl    AS  usdot_min_nav_cl,
          b.fed_agency    AS  usdot_fed_agency,
          b.submitted     AS  usdot_submitted,
          b.bridge_con    AS  usdot_bridge_con,
          b.lowest_rat    AS  usdot_lowest_rat,
          b.deck_area     AS  usdot_deck_area,
          b.status        AS  usdot_status,
          b.date          AS  usdot_date,
          b.latdd         AS  usdot_latdd,
          b.longdd        AS  usdot_longdd,

          ST_Distance(
            GEOGRAPHY(a.wkb_geometry),
            GEOGRAPHY(b.wkb_geometry)
          ) AS distance_meters,

          ST_MakeLine(a.wkb_geometry, b.wkb_geometry) AS wkb_geometry

        FROM nysdot_structures.bridges AS a
          FULL OUTER JOIN us_bureau_of_transportation_statistics.ny_bridge_inventory AS b
            -- NOTE: Cannot cast to integers because ids can be alpha-numeric.
            ON ( RegExp_Replace(b.structure, '^0{1,}', '') = a.bin )

        ORDER BY
            nysdot_bin,
            usdot_structure_008,
            distance_meters,
            nysdot_location_la DESC,
            nysdot_last_inspec DESC,
            nysdot_object_id,
            usdot_objectid
;

COMMIT ;
