export enum US_States {
  ak = "Alaska",
  al = "Alabama",
  ar = "Arkansas",
  az = "Arizona",
  ca = "California",
  co = "Colorado",
  ct = "Connecticut",
  dc = "District of Columbia",
  de = "Delaware",
  fl = "Florida",
  ga = "Georgia",
  hi = "Hawaii",
  ia = "Iowa",
  id = "Idaho",
  il = "Illinois",
  in = "Indiana",
  ks = "Kansas",
  ky = "Kentucky",
  la = "Louisiana",
  ma = "Massachusetts",
  md = "Maryland",
  me = "Maine",
  mi = "Michigan",
  mn = "Minnesota",
  mo = "Missouri",
  ms = "Mississippi",
  mt = "Montana",
  nc = "North Carolina",
  nd = "North Dakota",
  ne = "Nebraska",
  nh = "New Hampshire",
  nj = "New Jersey",
  nm = "New Mexico",
  nv = "Nevada",
  ny = "New York",
  oh = "Ohio",
  ok = "Oklahoma",
  or = "Oregon",
  pa = "Pennsylvania",
  pr = "Puerto Rico",
  ri = "Rhode Island",
  sc = "South Carolina",
  sd = "South Dakota",
  tn = "Tennessee",
  tx = "Texas",
  ut = "Utah",
  va = "Virginia",
  vt = "Vermont",
  wa = "Washington",
  wi = "Wisconsin",
  wv = "West Virginia",
  wy = "Wyoming",
}

export enum Canadian_Provinces {
  ab = "Alberta",
  bc = "British Columbia",
  mb = "Manitoba",
  nb = "New Brunswick",
  on = "Ontario",
  qc = "Quebec",
  sk = "Saskatchewan",
}

export const stateAbbreviationToName: Record<string, string> = {
  ...US_States,
  ...Canadian_Provinces,
};

export const stateNameToAbbreviation: Record<string, string> = Object.keys(
  stateAbbreviationToName
).reduce((acc, abbr) => {
  const name = stateAbbreviationToName[abbr];
  acc[name] = abbr;
  return acc;
}, {});
