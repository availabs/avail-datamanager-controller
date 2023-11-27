const { Client } = require('pg')
const pgFormat = require('pg-format')
const _ = require('lodash')

const omitted_columns = [
  "event_id",
  "aud_date_create",
  "aud_date_update",
  "aud_user_create",
  "aud_user_update",
  "shape_length",
  "to_date",
  "shape"
]

const table_schema = 'nysdot_milepoint_snapshot'

const table_names = [
    "ev_maint_snowplow_muni",
    "ev_maint_snowplow_state",
    "ev_pln_projectlocation",
    "ev_ris_acccontrol",
    "ev_ris_bike_ln_wid_p",
    "ev_ris_bike_ln_wid_r",
    "ev_ris_bus_ln_wid_p",
    "ev_ris_bus_ln_wid_r",
    "ev_ris_cctl_wid",
    "ev_ris_co_road",
    "ev_ris_collection_date",
    "ev_ris_cross_fall",
    "ev_ris_curve",
    "ev_ris_desc",
    "ev_ris_div",
    "ev_ris_fed_aid_primary",
    "ev_ris_functional_class",
    "ev_ris_grade",
    "ev_ris_hov",
    "ev_ris_hov_lanes",
    "ev_ris_hpms_ag_other",
    "ev_ris_hpms_ag_signal",
    "ev_ris_hpms_ag_signs",
    "ev_ris_hpms_base_thk",
    "ev_ris_hpms_base_type",
    "ev_ris_hpms_cntr_peak_ln",
    "ev_ris_hpms_crk_perc",
    "ev_ris_hpms_curves",
    "ev_ris_hpms_grades",
    "ev_ris_hpms_peak_ln",
    "ev_ris_hpms_peak_parking",
    "ev_ris_hpms_perc_green",
    "ev_ris_hpms_perc_sight",
    "ev_ris_hpms_sample",
    "ev_ris_hpms_shldr_type",
    "ev_ris_hpms_shldr_wid_l",
    "ev_ris_hpms_shldr_wid_r",
    "ev_ris_hpms_turn_ln_l",
    "ev_ris_hpms_turn_ln_r",
    "ev_ris_hpms_type_signal",
    "ev_ris_hpms_type_terr",
    "ev_ris_hpms_wide_ptnl",
    "ev_ris_hpms_widenobs",
    "ev_ris_hpms_yr_last_con",
    "ev_ris_hpms_yr_last_imp",
    "ev_ris_lf",
    "ev_ris_mpo",
    "ev_ris_maint_jur",
    "ev_ris_med_type",
    "ev_ris_med_wid",
    "ev_ris_misc_pvt_type_p",
    "ev_ris_misc_pvt_type_r",
    "ev_ris_misc_pvt_wid_p",
    "ev_ris_misc_pvt_wid_r",
    "ev_ris_muni",
    "ev_ris_nhfn",
    "ev_ris_nhs",
    "ev_ris_name",
    "ev_ris_one_way",
    "ev_ris_owner",
    "ev_ris_owning_jur",
    "ev_ris_pf",
    "ev_ris_parking_ln_wid_p",
    "ev_ris_parking_ln_wid_r",
    "ev_ris_pavement_layer",
    "ev_ris_pavement_type",
    "ev_ris_rail_crossing",
    "ev_ris_rdside_feat_l_p",
    "ev_ris_rdside_feat_l_r",
    "ev_ris_rdside_feat_r_p",
    "ev_ris_rdside_feat_r_r",
    "ev_ris_reservation",
    "ev_ris_residency",
    "ev_ris_scenic_byway",
    "ev_ris_shldr_l_wid_p",
    "ev_ris_shldr_l_wid_r",
    "ev_ris_shldr_r_wid_p",
    "ev_ris_shldr_r_wid_r",
    "ev_ris_speed_limit",
    "ev_ris_state_highway_num",
    "ev_ris_station",
    "ev_ris_station_mapping",
    "ev_ris_strahnet",
    "ev_ris_structure_type",
    "ev_ris_thru_ln_num_p",
    "ev_ris_thru_ln_num_r",
    "ev_ris_thru_ln_width_p",
    "ev_ris_thru_ln_width_r",
    "ev_ris_toll",
    "ev_ris_toll_fac",
    "ev_ris_toll_id",
    "ev_ris_trk_rte",
    "ev_ris_turn_ln_l_num_p",
    "ev_ris_turn_ln_l_num_r",
    "ev_ris_turn_ln_l_wid_p",
    "ev_ris_turn_ln_l_wid_r",
    "ev_ris_turn_ln_r_num_p",
    "ev_ris_turn_ln_r_num_r",
    "ev_ris_turn_ln_r_wid_p",
    "ev_ris_turn_ln_r_wid_r",
    "ev_ris_uac",
    "ev_str_bridge",
    "ev_str_tunnel",
    "ev_tradas_nyscountstats",
    // "redline" -- does not contain to_date
]

async function create_lrs_indices(db) {
  await db.query('BEGIN ;')

  for (const table_name of table_names) {
    console.log(table_name)

    const idx_name = `${table_name}_lrs_idx`

        // CREATE UNIQUE INDEX IF NOT EXISTS %I
    const sql = pgFormat(
      `
        CREATE INDEX IF NOT EXISTS %I
          ON %I.%I (to_date, route_id, from_measure, to_measure)
        ;

        CLUSTER %I.%I USING %I ;
      `,
      // CREATE
      idx_name,
      table_schema,
      table_name,

      // CLUSTER
      table_schema,
      table_name,
      idx_name,
    )

    await db.query(sql)
  }

  await db.query('COMMIT ;')
}

async function collectMetadata(db, layers, lrs_segment) {

  const {
    route_id,
    from_measure = null,
    to_measure = null
  } = lrs_segment

  const properties = {
    route_id,
    from_measure,
    to_measure,
    layers: {
    }
  }

  for (const table_name of layers) {
    const { fields } = await db.query(pgFormat(`SELECT * FROM %I.%I WHERE false ;`, table_schema, table_name))

    const cols = _.difference(fields.map(({name}) => name.toLowerCase()), ['route_id', ...omitted_columns])
    const col_placeholders = cols.map(() => '%I')

    const sql = pgFormat(`
      SELECT ${col_placeholders}
      FROM %I.%I
      WHERE (
        ( to_date IS NULL )
        AND
        ( route_id = $1 )
        AND
        (
          numrange(from_measure::NUMERIC, to_measure::NUMERIC)
          &&
          numrange(
            $2::NUMERIC,
            $3::NUMERIC,
            '[]'
          )
        )
      )
    `, ...cols, table_schema, table_name
     )

     const { rows } = await db.query(sql, [route_id, from_measure, to_measure])

     properties.layers[table_name] = rows
  }

  return properties
}

async function main() {
  const db = new Client()

  try {
    await db.connect()
    // await create_lrs_indices(db)
    const meta = await collectMetadata(db, table_names, { route_id: '100011031', from_measure: 1, to_measure: 2 })

    console.log(JSON.stringify(meta, null, 4))
  } catch (err) {
    console.error(err)
  } finally {
    await db.end()
  }
}

main()
