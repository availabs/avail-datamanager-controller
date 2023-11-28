// See [FIRM Database Technical Reference](https://www.fema.gov/sites/default/files/2020-02/FIRM_Database_Technical_Reference_Feb_2019.pdf)
//   Table 13: S_Fld_Haz_Ar
const gdb_fields = {
  dfirm_id: "text",
  version_id: "text",
  fld_ar_id: "text",
  study_typ: "text",
  fld_zone: "text",
  zone_subty: "text",
  sfha_tf: "text",
  static_bfe: "double",
  v_datum: "text",
  depth: "double",
  len_unit: "text",
  velocity: "double",
  vel_unit: "text",
  ar_revert: "text",
  ar_subtrv: "text",
  bfe_revert: "double",
  dep_revert: "double",
  dual_zone: "text",
  source_cit: "text",
  gfid: "text",

  // NOTE: shape_leng and shape_area are not part of the FIRM specificaion.
  shape_leng: "double",
  shape_area: "double",
};

const required_fields = Object.keys(gdb_fields).map((f) => f.toUpperCase());

module.exports = {
  gdb_fields,
  required_fields,
};
