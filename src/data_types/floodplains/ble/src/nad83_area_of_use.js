const assert = require("assert").strict;

const srs_to_counties = {
  // https://epsg.io/6541
  6541: [
    "Allegany",
    "Cattaraugus",
    "Chautauqua",
    "Erie",
    "Genesee",
    "Livingston",
    "Monroe",
    "Niagara",
    "Orleans",
    "Wyoming",
  ],

  // https://epsg.io/6535
  6535: [
    "Broome",
    "Cayuga",
    "Chemung",
    "Chenango",
    "Cortland",
    "Jefferson",
    "Lewis",
    "Madison",
    "Oneida",
    "Onondaga",
    "Ontario",
    "Oswego",
    "Schuyler",
    "Seneca",
    "Steuben",
    "Tioga",
    "Tompkins",
    "Wayne",
    "Yates",
  ],

  // https://epsg.io/6539
  6539: [
    "Bronx",
    "Kings",
    "Nassau",
    "New York",
    "Queens",
    "Richmond",
    "Suffolk",
  ],

  // https://epsg.io/6537
  6537: [
    "Albany",
    "Clinton",
    "Columbia",
    "Delaware",
    "Dutchess",
    "Essex",
    "Franklin",
    "Fulton",
    "Greene",
    "Hamilton",
    "Herkimer",
    "Montgomery",
    "Orange",
    "Otsego",
    "Putnam",
    "Rensselaer",
    "Rockland",
    "Saratoga",
    "Schenectady",
    "Schoharie",
    "St Lawrence",
    "Sullivan",
    "Ulster",
    "Warren",
    "Washington",
    "Westchester",
  ],
};

const county_name_to_fips = {
  Albany: "36001",
  Allegany: "36003",
  Bronx: "36005",
  Broome: "36007",
  Cattaraugus: "36009",
  Cayuga: "36011",
  Chautauqua: "36013",
  Chemung: "36015",
  Chenango: "36017",
  Clinton: "36019",
  Columbia: "36021",
  Cortland: "36023",
  Delaware: "36025",
  Dutchess: "36027",
  Erie: "36029",
  Essex: "36031",
  Franklin: "36033",
  Fulton: "36035",
  Genesee: "36037",
  Greene: "36039",
  Hamilton: "36041",
  Herkimer: "36043",
  Jefferson: "36045",
  Kings: "36047",
  Lewis: "36049",
  Livingston: "36051",
  Madison: "36053",
  Monroe: "36055",
  Montgomery: "36057",
  Nassau: "36059",
  "New York": "36061",
  Niagara: "36063",
  Oneida: "36065",
  Onondaga: "36067",
  Ontario: "36069",
  Orange: "36071",
  Orleans: "36073",
  Oswego: "36075",
  Otsego: "36077",
  Putnam: "36079",
  Queens: "36081",
  Rensselaer: "36083",
  Richmond: "36085",
  Rockland: "36087",
  "St Lawrence": "36089",
  Saratoga: "36091",
  Schenectady: "36093",
  Schoharie: "36095",
  Schuyler: "36097",
  Seneca: "36099",
  Steuben: "36101",
  Suffolk: "36103",
  Sullivan: "36105",
  Tioga: "36107",
  Tompkins: "36109",
  Ulster: "36111",
  Warren: "36113",
  Washington: "36115",
  Wayne: "36117",
  Westchester: "36119",
  Wyoming: "36121",
  Yates: "36123",
};

const fips_to_srs = Object.keys(srs_to_counties).reduce((acc, srs) => {
  for (const county of srs_to_counties[srs]) {
    console.log(county);
    const fips = county_name_to_fips[county];

    assert(!!fips);

    acc[fips] = `${srs}`;
  }
  return acc;
}, {});

const case_clauses = Object.keys(fips_to_srs)
  .sort()
  .map((fips) => `WHEN '${fips}' THEN ${fips_to_srs[fips]}`);

const case_statement = `
  CASE fips
    ${case_clauses.join("\n    ")}
  END
`;

console.log(case_statement);
