#!/usr/bin/env node

const { writeFileSync, unlinkSync, existsSync } = require("fs");
const { join } = require("path");

const _ = require('lodash');

const gdbs_meta = require("../gdbs_meta.json");

const analysis_out_fpath = join(__dirname, '../gdbs_layer_analysis.json')
const summary_out_fpath = join(__dirname, '../gdbs_layer_summary')

const gdb_names = Object.keys(gdbs_meta).filter(gdb_name => !/^2018/.test(gdb_name));

function analyzeGDBsMeta() {
  const meta_obj = {}

  for (const gdb_name of gdb_names) {
    const { layers } = gdbs_meta[gdb_name];


    for (const { name : layer_name, geometryFields, fields, featureCount : feature_count } of layers) {
      meta_obj[layer_name] = meta_obj[layer_name] || {
        gdb_meta: [],
        fields_meta: {},
        geom_meta: {}
      }

      const {
        gdb_meta, fields_meta, geom_meta
      } = meta_obj[layer_name]

      gdb_meta.push({
        gdb_name,
        feature_count
      })

      for (const { name : geom_name, type : geom_type } of geometryFields) {
        geom_meta[geom_name] = geom_meta[geom_name] || {}

        geom_meta[geom_name][geom_type] = geom_meta[geom_name][geom_type] || []

        geom_meta[geom_name][geom_type].push( gdb_name)
      }

      for (const { name : field_name, type : field_type } of fields) {
        fields_meta[field_name] = fields_meta[field_name] || {}
        fields_meta[field_name][field_type] = fields_meta[field_name][field_type] || []

        fields_meta[field_name][field_type].push(gdb_name)
      }
    }
  }

  const meta_arr = Object.keys(meta_obj)
    .sort()
    .map(layer_name => {
      const { gdb_meta, fields_meta, geom_meta } = meta_obj[layer_name]

      const property_fields = Object.keys(fields_meta)
        .map(field_name => ({
          field_name,
          schema_analysis: Object.keys(fields_meta[field_name])
            .map(field_type => ({
              field_type,
              gdb_names: fields_meta[field_name][field_type],
              is_consistent: _.isEqual(gdb_names, fields_meta[field_name][field_type])
            }))
        }))

      const geometry_fields = Object.keys(geom_meta)
        .map(field_name => ({
          field_name,
          schema_analysis: Object.keys(geom_meta[field_name])
            .map(field_type => ({
              field_type,
              gdb_names: geom_meta[field_name][field_type],
              is_consistent: _.isEqual(gdb_names, geom_meta[field_name][field_type]),
              is_linestring: /linestring/i.test(field_type),
            }))
        }))

      const has_gis_id = property_fields.some(({ field_name, schema_analysis }) => {
        return /gis_id/i.test(field_name)

        // if (!/gis_id/i.test(field_name)) {
        //   return false
        // }

        // const gdbs_with_gis_id = _.flatten(schema_analysis.map(({ gdb_names }) => gdb_names)).sort()

        // return _.isEqual(gdbs_with_gis_id, gdb_names)
      })

      const has_dot_id = property_fields.some(({ field_name, schema_analysis }) => {
        return /dot_id/i.test(field_name)

        // if (!/dot_id/i.test(field_name)) {
        //   return false
        // }

        // const gdbs_with_dot_id = _.flatten(schema_analysis.map(({ gdb_names }) => gdb_names)).sort()

        // return _.isEqual(gdbs_with_dot_id, gdb_names)
      })

      const property_field_names = property_fields.map(({ field_name }) => field_name.toLowerCase())
      const has_lrs_id = _.intersection(['route_id', 'from_measure', 'to_measure'], property_field_names).length === 3

      const has_geometry = !!geometry_fields.length

      const has_linestring_geom = geometry_fields
        .some(
          ({ schema_analysis }) => schema_analysis.some(({ is_linestring }) => is_linestring)
        )

      return {
        layer_name,
        has_gis_id,
        has_dot_id,
        has_lrs_id,
        has_geometry,
        has_linestring_geom,
        property_fields,
        geometry_fields,
      }
    })

  return meta_arr
}

function summarizeAnalysis(analysis) {
  const linestring_layers_summary = analysis.map(({
        layer_name,
        has_gis_id,
        has_dot_id,
        has_linestring_geom,
  }) => ({
        layer_name,
        has_gis_id,
        has_dot_id,
        has_linestring_geom,
  }))
    .filter(({has_linestring_geom}) => has_linestring_geom)
    .map(({
        layer_name,
        has_gis_id,
        has_dot_id,
    }) => ({
        layer_name,
        has_gis_id,
        has_dot_id,
    }))

  const layers_with_global_id = analysis.map(({
        layer_name,
        has_gis_id,
        has_dot_id,
        has_linestring_geom,
  }) => ({
        layer_name,
        has_gis_id,
        has_dot_id,
        has_linestring_geom,
  }))
    .filter(({
      has_gis_id,
      has_dot_id,
    }) => has_gis_id || has_dot_id )
  ;

  const layers_with_lrs_id = analysis.filter(({ has_lrs_id }) => has_lrs_id).map(({ layer_name }) => layer_name)

  const linestring_layers_with_global_id = linestring_layers_summary
    .filter(({
      has_gis_id,
      has_dot_id,
    }) => has_gis_id || has_dot_id )

  const linestring_layers_without_global_id =
      _.difference(linestring_layers_summary, linestring_layers_with_global_id)
      .map(({ layer_name }) => layer_name)


  const summary = {
    linestring_layers_with_global_id,
    linestring_layers_without_global_id,
    layers_with_lrs_id
  }

  return summary
}

function main() {
  const analysis = analyzeGDBsMeta()

  const summary = summarizeAnalysis(analysis)

  console.log(JSON.stringify(summary.layers_with_lrs_id, null, 4))

  writeFileSync(analysis_out_fpath, JSON.stringify(analysis, null, 4))
  writeFileSync(summary_out_fpath, JSON.stringify(summary, null, 4))
}

// console.log(JSON.stringify(getGdbLayersMeta(), null, 4));

main()
