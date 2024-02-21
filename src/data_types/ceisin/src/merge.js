#!/usr/bin/env node

const execa = require('execa')
const fs = require('fs')
const path = require('path')

const initial_data_dir = path.join(__dirname, '../initial_data')
const derived_data_dir = path.join(__dirname, '../derived_data')

fs.mkdirSync(derived_data_dir, { recursive: true })

const output_gpkg_path = path.join(derived_data_dir, 'ceisin.gpkg')
const output_layer_name = 'ceisin_building_footprints'

const dataset_name_suffix = '_county_building_footprints.zip'

// http://fidss.ciesin.columbia.edu/building_data_adaptation
const county_names = [
  'Albany',
  'Allegany',
  'Broome',
  'Cattaraugus',
  'Cayuga',
  'Chautauqua',
  'Chemung',
  'Chenango',
  'Clinton',
  'Columbia',
  'Cortland',
  'Delaware',
  'Dutchess',
  'Erie',
  'Essex',
  'Franklin',
  'Fulton',
  'Genesee',
  'Greene',
  'Hamilton',
  'Herkimer',
  'Jefferson',
  'Lewis',
  'Livingston',
  'Madison',
  'Monroe',
  'Montgomery',
  'Nassau',
  'Niagara',
  'Oneida',
  'Onondaga',
  'Ontario',
  'Orange',
  'Orleans',
  'Oswego',
  'Otsego',
  'Putnam',
  'Rensselaer',
  'Rockland',
  'Saratoga',
  'Schenectady',
  'Schoharie',
  'Schuyler',
  'Seneca',
  'Steuben',
  'St Lawrence',
  'Suffolk',
  'Sullivan',
  'Tioga',
  'Tompkins',
  'Ulster',
  'Warren',
  'Washington',
  'Wayne',
  'Westchester',
  'Wyoming',
  'Yates',
]

const dataset_paths = county_names.sort().map(c => path.join(initial_data_dir, `${c}${dataset_name_suffix}`))

async function main() {

  const gdal_vsi_paths = dataset_paths.map(dpath => `/vsizip/${dpath}`)

  await execa(
    'ogrmerge.py',
    [
      '-f', 'GPKG',
      '-o', output_gpkg_path,
      '-overwrite_ds',
      '-single',
      '-nln', output_layer_name,
      ...gdal_vsi_paths
    ],
    { stdio: 'inherit' }
  )
}

main()
