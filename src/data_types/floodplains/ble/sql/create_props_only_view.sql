CREATE OR REPLACE VIEW floodplains.merged_ble_props_only
  AS
    SELECT
        objectid,
        dfirm_id,
        version_id,
        fld_ar_id,
        study_typ,
        fld_zone,
        zone_subty,
        sfha_tf,
        static_bfe,
        v_datum,
        depth,
        len_unit,
        velocity,
        vel_unit,
        ar_revert,
        ar_subtrv,
        bfe_revert,
        dep_revert,
        dual_zone,
        source_cit,
        shape_leng,
        shape_area,
        gfid
      FROM floodplains.merged_ble
;
