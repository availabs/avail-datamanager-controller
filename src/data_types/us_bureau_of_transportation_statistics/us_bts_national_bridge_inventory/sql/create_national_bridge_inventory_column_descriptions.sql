-- https://fgdl.org/zips/metadata/htm/bts_bridge_jun17.htm

CREATE TABLE IF NOT EXISTS
  us_bureau_of_transportation_statistics.national_bridge_inventory_column_descriptions (
    column_name   TEXT PRIMARY KEY,
    description   TEXT NOT NULL
  );

INSERT INTO us_bureau_of_transportation_statistics.national_bridge_inventory_column_descriptions
  VALUES 
    ( 'objectid',     'Internal feature number' ),

    ( 'state_code',   'State Code' ),

    ( 'structure_',   'Structure Number' ),

    ( 'record_typ',   'Record Type' ),

    ( 'route_pref',   'Route Signing Prefix' ),

    ( 'service_le',   'Designated Level of Service' ),

    ( 'route_numb',   'Route Number' ),

    ( 'direction_',   'Directional Suffix' ),

    ( 'highway_di',   'Highway Agency District' ),

    ( 'county_cod',   'County (Parish) Code' ),

    ( 'place_code',   'Place Code' ),

    ( 'features_d',   'Features Intersected' ),

    ( 'critical_f',   'Critical Facility Indicator' ),

    ( 'facility_c',   'Facility Carried By Structure' ),

    ( 'location_0',   'Location' ),

    ( 'min_vert_c',   'Inventory Route, Min Vertical Clearance' ),

    ( 'kilopoint_',   'Kilometerpoint' ),

    ( 'base_hwy_n',   'Base Highway Network' ),

    ( 'lrs_inv_ro',   'LRS Inventory Route' ),

    ( 'subroute_n',   'Subroute Number' ),

    ( 'lat_016',      'Latitude' ),

    ( 'long_017',     'Longitude' ),

    ( 'detour_kil',   'Bypass/Detour Length' ),

    ( 'toll_020',     'Toll' ),

    ( 'maintenanc',   'Maintenance Responsibility' ),

    ( 'owner_022',    'Owner' ),

    ( 'functional',   'Functional Class of Inventory Route' ),

    ( 'year_built',   'Year Built' ),

    ( 'traffic_la',   'Lanes on Structure' ),

    ( 'traffic__1',   'Lanes Under Structure' ),

    ( 'adt_029',      'Average Daily Traffic' ),

    ( 'year_adt_0',   'Year of Average Daily Traffic' ),

    ( 'design_loa',   'Design Load' ),

    ( 'appr_width',   'Approach Roadway Width' ),

    ( 'median_cod',   'Bridge Median' ),

    ( 'degrees_sk',   'Skew' ),

    ( 'structure1',   'Structure Flared' ),

    ( 'railings_0',   'Traffic Safety Features' ),

    ( 'transition',   'Transitions' ),

    ( 'appr_rail_',   'Approach Guardrail' ),

    ( 'appr_rail1',   'Approach Guardrail Ends' ),

    ( 'history_03',   'Historical Significance' ),

    ( 'navigation',   'Navigation Control' ),

    ( 'nav_vert_c',   'Navigation Vertical Clearance' ),

    ( 'nav_horr_c',   'Navigation Horizontal Clearance' ),

    ( 'open_close',   'Strucutre Open/Posted/Closed' ),

    ( 'service_on',   'Type of Service on Bridge' ),

    ( 'service_un',   'Type of Service Under Bridge' ),

    ( 'structur_1',   'Kind of Material/Design' ),

    ( 'structur_2',   'Type of Design/Construction' ),

    ( 'appr_kind_',   'Approach Spans, Kind of Material/Design' ),

    ( 'appr_type_',   'Approach Spans, Type of Design/Construction' ),

    ( 'main_unit_',   'Number of Spans in Main Unit' ),

    ( 'appr_spans',   'Number of Approach Spans' ),

    ( 'horr_clr_m',   'Inventory Rte Total Horizontal Clearance' ),

    ( 'max_span_l',   'Length of Maximum Span' ),

    ( 'structur_3',   'Structure Length' ),

    ( 'left_curb_',   'Left Curb/Sidewalk Width' ),

    ( 'right_curb',   'Right Curb/Sidewalk Width' ),

    ( 'roadway_wi',   'Bridge Roadway Width Curb-To-Curb' ),

    ( 'deck_width',   'Deck Width, Out-To-Out' ),

    ( 'vert_clr_o',   'Min Vertical Clearance Over Bridge Roadway' ),

    ( 'vert_clr_u',   'Minimum Vertical Underclearance Reference Feature' ),

    ( 'vert_clr_1',   'Minimum Vertical Underclearance' ),

    ( 'lat_und_re',   'Min Lateral Underclear on Right, Reference Feature' ),

    ( 'lat_und_mt',   'Minimum Lateral Underclearance' ),

    ( 'left_lat_u',   'Min Lateral Underclear On Left' ),

    ( 'deck_cond_',   'Deck' ),

    ( 'superstruc',   'Superstructure' ),

    ( 'substructu',   'Substructure' ),

    ( 'channel_co',   'Channel/Channel Protection' ),

    ( 'culvert_co',   'Culverts' ),

    ( 'opr_rating',   'Method Used to Determine Operating Rating' ),

    ( 'operating_',   'Operating Rating' ),

    ( 'inv_rating',   'Method Used to Determine Inventory Rating' ),

    ( 'inventory_',   'Inventory Rating' ),

    ( 'structural',   'Structural Evaluation' ),

    ( 'deck_geome',   'Deck Geometry' ),

    ( 'undclrence',   'Underclear, Vertical & Horizontal' ),

    ( 'posting_ev',   'Bridge Posting' ),

    ( 'waterway_e',   'Waterway Adequacy' ),

    ( 'appr_road_',   'Approach Road Alignment' ),

    ( 'work_propo',   'Type of Work Proposed' ),

    ( 'work_done_',   'Work Done By' ),

    ( 'imp_len_mt',   'Length of Structure Improvement' ),

    ( 'date_of_in',   'Inspection Date' ),

    ( 'inspect_fr',   'Designated Inspection Frequency' ),

    ( 'fracture_0',   'Fracture Critical Details' ),

    ( 'undwater_l',   'Underwater Inspection' ),

    ( 'spec_inspe',   'Other Special Inspection' ),

    ( 'fracture_l',   'Fracture Critical Details Date' ),

    ( 'undwater_1',   'Underwater Inspection Date' ),

    ( 'spec_last_',   'Other Special Inspection Date' ),

    ( 'bridge_imp',   'Bridge Improvement Cost' ),

    ( 'roadway_im',   'Roadway Improvement Cost' ),

    ( 'total_imp_',   'Total Project Cost' ),

    ( 'year_of_im',   'Year Of Improvement Cost Estimate' ),

    ( 'other_stat',   'Neighboring State Code' ),

    ( 'other_st_1',   'Percent Responsibility' ),

    ( 'othr_state',   'Border Bridge Structure Number' ),

    ( 'strahnet_h',   'STRAHNET Highway Designation' ),

    ( 'parallel_s',   'Parallel Structure Designation' ),

    ( 'traffic_di',   'Direction Of Traffic' ),

    ( 'temp_struc',   'Temporary Structure Designation' ),

    ( 'highway_sy',   'Highway System Of Inventory Route' ),

    ( 'federal_la',   'Federal Lands Highways' ),

    ( 'year_recon',   'Year Reconstructed' ),

    ( 'deck_struc',   'Deck Structure Type' ),

    ( 'surface_ty',   'Type of Wearing Surface' ),

    ( 'membrane_t',   'Type of Membrane' ),

    ( 'deck_prote',   'Deck Protection' ),

    ( 'percent_ad',   'Average daily truck traffic' ),

    ( 'national_n',   'Designated national network' ),

    ( 'pier_prote',   'Pier/Abutment protection' ),

    ( 'bridge_len',   'NBIS bridge length' ),

    ( 'scour_crit',   'Scour critical bridges' ),

    ( 'future_adt',   'Future average daily traffic' ),

    ( 'year_of_fu',   'Year of future average daily traffic' ),

    ( 'fed_agency',   'No description' ),

    ( 'date_last_',   'No description' ),

    ( 'type_last_',   'No description' ),

    ( 'deduct_cod',   'No description' ),

    ( 'remarks',      'No description' ),

    ( 'program_co',   'No description' ),

    ( 'proj_no',      'No description' ),

    ( 'proj_suffi',   'No description' ),

    ( 'nbi_type_o',   'No description' ),

    ( 'dtl_type_o',   'No description' ),

    ( 'special_co',   'No description' ),

    ( 'step_code',    'No description' ),

    ( 'status_wit',   'No description' ),

    ( 'sufficienc',   'No description' ),

    ( 'sufficie_1',   'No description' ),

    ( 'status_no_',   'No description' ),

    ( 'min_nav_cl',   'Minimum navigation vertical clearance vertical lift bridge' ),

    ( 'cat10',        'Bridge Condition' ),

    ( 'cat23',        'Condition code value that dictates the overall Bridge Condition' ),

    ( 'cat29',        'Deck area as defined by the Pavement and Bridge Condition Performance Measures final rule, published in January of 2017' ),

    ( 'lat_dd',       'Latitude Decimal Degrees' ),

    ( 'long_dd',      'Longitude Decimal Degrees' ),

    ( 'mgrs',         'Military Grid Reference System (MGRS) Coordinate of the Facility. The MGRS is the geocoordinate standard used by NATO militaries for locating points on the earth. The MGRS provides a means to represent any location on the surface of the Earth using an alphanumeric string. Hierarchical references are based on the Universal Transverse Mercator (UTM) coordinate system. The MGRS is used for the entire earth. http://mgrs-data.org/' ),

    ( 'googlemap',    'Webpage hyperlink to location in Google Maps.' ),

    ( 'descript',     'GeoPlan added field based on [FEATURES_D], [FACILITY_C], and [LOCATION_0]' ),

    ( 'fgdlaqdate',   'GeoPlan added field based on date acquired from source' ),

    ( 'autoid',       'Unique ID added by GeoPlan' )

  ON CONFLICT DO NOTHING
;
