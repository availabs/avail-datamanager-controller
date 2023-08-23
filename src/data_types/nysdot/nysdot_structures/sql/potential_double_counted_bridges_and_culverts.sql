DROP TABLE IF EXISTS nysdot_structures.possibly_double_counted_bridges_and_culverts ;

CREATE TABLE nysdot_structures.possibly_double_counted_bridges_and_culverts
  AS
    SELECT
            a.ogc_fid               AS  bridge_ogc_fid,
            a.object_id             AS  bridge_object_id,
            a.bin                   AS  bridge_bin,
            a.location_la           AS  bridge_location_la,
            a.carried               AS  bridge_carried,
            a.crossed               AS  bridge_crossed,
            a.primary_own           AS  bridge_primary_own,
            a.primary_mai           AS  bridge_primary_mai,
            a.county                AS  bridge_county,
            a.region                AS  bridge_region,
            a.gtms_struct           AS  bridge_gtms_struct,
            a.gtms_materi           AS  bridge_gtms_materi,
            a.number_of_sp          AS  bridge_number_of_sp,
            a.condition_r           AS  bridge_condition_r,
            a.last_inspec           AS  bridge_last_inspec,
            a.bridge_leng           AS  bridge_bridge_leng,
            a.deck_area_sq          AS  bridge_deck_area_sq,
            a.aadt                  AS  bridge_aadt,
            a.year_built            AS  bridge_year_built,
            a.posted_load           AS  bridge_posted_load,
            a.r_posted              AS  bridge_r_posted,
            a.other                 AS  bridge_other,
            a.redc                  AS  bridge_redc,
            a.nbi_deck_co           AS  bridge_nbi_deck_co,
            a.nbi_substr            AS  bridge_nbi_substr,
            a.nbi_supers            AS  bridge_nbi_supers,
            a.fhwa_condi            AS  bridge_fhwa_condi,
            ST_X(a.wkb_geometry)    AS  bridge_longitude,
            ST_Y(a.wkb_geometry)    AS  bridge_latitude,

            b.ogc_fid               AS  culvert_ogc_fid,
            b.object_id             AS  culvert_object_id,
            b.bin                   AS  culvert_bin,
            b.location_la           AS  culvert_location_la,
            b.crossed               AS  culvert_crossed,
            b.primary_own           AS  culvert_primary_own,
            b.primary_mai           AS  culvert_primary_mai,
            b.county                AS  culvert_county,
            b.region                AS  culvert_region,
            b.residency             AS  culvert_residency,
            b.gtm_sstruct           AS  culvert_gtm_sstruct,
            b.gtms_materi           AS  culvert_gtms_materi,
            b.condition_r           AS  culvert_condition_r,
            b.last_inspec           AS  culvert_last_inspec,
            b.route                 AS  culvert_route,
            b.reference_m           AS  culvert_reference_m,
            b.type_max_spa          AS  culvert_type_max_spa,
            b.year_built            AS  culvert_year_built,
            b.abutment_ty           AS  culvert_abutment_ty,
            b.stream_bed_m          AS  culvert_stream_bed_m,
            b.maintenanc            AS  culvert_maintenanc,
            b.abutment_he           AS  culvert_abutment_he,
            b.culvert_ske           AS  culvert_culvert_ske,
            b.out_to_out_wi         AS  culvert_out_to_out_wi,
            b.number_of_sp          AS  culvert_number_of_sp,
            b.span_length           AS  culvert_span_length,
            b.structure_l           AS  culvert_structure_l,
            b.general_rec           AS  culvert_general_rec,
            b.redc                  AS  culvert_redc,
            ST_X(b.wkb_geometry)    AS  culvert_longitude,
            ST_Y(b.wkb_geometry)    AS  culvert_latitude,

            ST_Distance(
              GEOGRAPHY(a.wkb_geometry),
              GEOGRAPHY(b.wkb_geometry)
            ) AS distance_between_meters,

            ST_MakeLine(a.wkb_geometry, b.wkb_geometry) AS wkb_geometry

      FROM nysdot_structures.bridges AS a
        INNER JOIN nysdot_structures.culverts AS b
          ON (
            ST_Distance(
              GEOGRAPHY(a.wkb_geometry),
              GEOGRAPHY(b.wkb_geometry)
            ) < 250
          )
      WHERE ( gtms_struct = '19 - Culvert' )
;
