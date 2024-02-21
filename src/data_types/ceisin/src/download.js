#!/usr/bin/env node

const execa = require('execa')
const path = require('path')

const initial_data_dir = path.join(__dirname, '../initial_data')

// http://fidss.ciesin.columbia.edu/fidss_files/zips/St%20Lawrence_county_building_footprints.zip
const link_addr_template = 'http://fidss.ciesin.columbia.edu/fidss_files/zips/__COUNTY_NAME___county_building_footprints.zip'

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

const links = county_names.map(c => link_addr_template.replace(/__COUNTY_NAME__/, c.replace(/ /g, '%20')))

async function main() {
  for (const link of links) {
    await execa(
      'wget',
      [
        '--directory-prefix', initial_data_dir,
        link
      ],
      { stdio: 'inherit' }
    )

    await new Promise(resolve => setTimeout(resolve, 3000))
  }
}

main()
