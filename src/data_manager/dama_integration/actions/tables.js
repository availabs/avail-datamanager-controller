const tables = {
    details: {
        name: 'details',
        schema: 'severe_weather_new',
        columns: [
            'begin_yearmonth', 'begin_day', 'begin_time', 'end_yearmonth', 'end_day', 'end_time', 'episode_id', 'event_id',
            'state', 'state_fips', 'year', 'month_name', 'event_type', 'cz_type', 'cz_fips', 'cz_name', 'wfo', 'begin_date_time',
            'cz_timezone', 'end_date_time', 'injuries_direct', 'injuries_indirect', 'deaths_direct', 'deaths_indirect', 'damage_property',
            'damage_crops', 'source', 'magnitude', 'magnitude_type', 'flood_cause', 'category', 'tor_f_scale', 'tor_length', 'tor_width',
            'tor_other_wfo', 'tor_other_cz_state', 'tor_other_cz_fips', 'tor_other_cz_name', 'begin_range', 'begin_azimuth', 'begin_location',
            'end_range', 'end_azimuth', 'end_location', 'begin_lat', 'begin_lon', 'end_lat', 'end_lon', 'episode_narrative', 'event_narrative',
            'data_source', 'begin_coords_geom', 'end_coords_geom', 'property_damage', 'crop_damage', 'geoid', 'cousub_geoid'
        ],
        numericColumns: [
            'begin_yearmonth', 'begin_day', 'begin_time', 'end_yearmonth', 'end_day', 'end_time', 'episode_id', 'event_id',
            'state_fips', 'year', 'cz_fips', 'injuries_direct', 'injuries_indirect',
            'deaths_direct', 'deaths_indirect', 'begin_range', 'end_range',
        ],
        floatColumns: [
            'magnitude', 'tor_length', 'tor_width', 'begin_lat', 'begin_lon', 'end_lat', 'end_lon'
        ],
        dateColumns: [
           'begin_date_time', 'end_date_time'
        ],
    },
    fatalities: {
        name: 'fatalities',
        schema: 'severe_weather_new',
        columns: [
            'fat_yearmonth', 'fat_day', 'fat_time', 'fatality_id', 'event_id', 'fatality_age', 'event_yearmonth',
            'fatality_type', 'fatality_sex', 'fatality_location', 'fatality_date'
        ],
        numericColumns: [
            'fat_yearmonth', 'fat_day', 'fat_time', 'fatality_id', 'event_id', 'fatality_age', 'event_yearmonth'
        ],
        floatColumns: [

        ],
        dateColumns: [
            'fatality_date',
        ],
    },
    locations: {
        name: 'locations',
        schema: 'severe_weather_new',
        columns: [
            'yearmonth', 'episode_id', 'event_id', 'location_index', 'range', 'latitude', 'longitude', 'lat2', 'lon2',
            'azimuth', 'location', 'coords_geom'
        ],
        numericColumns: [
            'yearmonth', 'episode_id', 'event_id', 'location_index', 'lat2', 'lon2'
        ],
        floatColumns: [
            'range', 'latitude', 'longitude',
        ],
        other: [
            'coords_geom'
        ],
    },
}


module.exports = {
    tables
}