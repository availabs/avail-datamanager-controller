CREATE SCHEMA IF NOT EXISTS _data_manager_admin ;

-- https://docs.mapbox.com/mapbox-gl-js/style-spec/layers/#type

CREATE TABLE IF NOT EXISTS _data_manager_admin.default_mapbox_paint_styles (
  mapbox_type     TEXT PRIMARY KEY,
  mapbox_paint_style     JSONB NOT NULL
) ;

INSERT INTO _data_manager_admin.default_mapbox_paint_styles (
  mapbox_type,
  mapbox_paint_style
) VALUES 
  (
    'circle',
    '
      {
        "circle-color": "#B42222",
        "circle-radius": 6
      }
    '::JSONB
  ),
  (
    'line',
    '
      {
        "line-color": "#64748b",
        "line-width": 2
      }
    '::JSONB
  ),
  (
    'fill',
    '
      {
        "fill-color": "#0080ff",
        "fill-opacity": 0.5
      }
    '::JSONB
  )
  ON CONFLICT ( mapbox_type ) DO NOTHING
;

CREATE TABLE IF NOT EXISTS _data_manager_admin.geojson_type_to_mapbox_type (
  geojson_type    TEXT PRIMARY KEY,
  mapbox_type     TEXT
) ;

INSERT INTO _data_manager_admin.geojson_type_to_mapbox_type (
  geojson_type,
  mapbox_type
) VALUES 
  (
    'LineString',
    'line'
  ),
  (
    'MultiLineString',
    'line'
  ),
  (
    'Point',
    'circle'
  ),
  (
    'MultiPoint',
    'circle'
  ),
  (
    'Polygon',
    'fill'
  ),
  (
    'MultiPolygon',
    'fill'
  )
  ON CONFLICT ( geojson_type ) DO NOTHING
;

CREATE OR REPLACE VIEW _data_manager_admin.geojson_type_default_mapbox_paint_styles
  AS
    SELECT
        *
      FROM _data_manager_admin.default_mapbox_paint_styles AS a
        INNER JOIN _data_manager_admin.geojson_type_to_mapbox_type AS b
          USING (mapbox_type)
;

CREATE TABLE IF NOT EXISTS _data_manager_admin.dama_sources_bespoke_mapbox_paint_style (
  source_id             INTEGER PRIMARY KEY,
  mapbox_type           TEXT NOT NULL,
  mapbox_paint_style    JSONB NOT NULL
) ;


-- Users can create bespoke styles for a view.
CREATE TABLE IF NOT EXISTS _data_manager_admin.dama_views_bespoke_mapbox_paint_style (
  view_id               INTEGER PRIMARY KEY,
  mapbox_type           TEXT NOT NULL,
  mapbox_paint_style    JSONB NOT NULL
) ;

CREATE TABLE IF NOT EXISTS _data_manager_admin.dama_views_mbtiles_metadata (
  mbtiles_id          SERIAL PRIMARY KEY,
  view_id             INTEGER,

  tileset_timestamp   TIMESTAMP,
  tileset_name        TEXT NOT NULL,

  source_id           TEXT NOT NULL,
  source_layer_name   TEXT NOT NULL,
  source_type         TEXT NOT NULL,

  tippecanoe_args     JSONB NOT NULL,
  tippecanoe_filter   JSONB,

  UNIQUE ( view_id, tileset_timestamp )
) ;

-- Prefers bespoke, falls back to default.
CREATE OR REPLACE VIEW _data_manager_admin.dama_views_mapbox_comprehensive
  AS
    SELECT
        t.*,

        jsonb_build_object(
          'layers',
          jsonb_build_array(
            jsonb_build_object(
              'id',
              t.source_id,

              'type',
              t.mapbox_type,

              'paint',
              t.mapbox_paint_style,

              'source',
              t.source_id,

              'source-layer',
              t.source_layer_name
            )
          ),

          'sources',
          jsonb_build_array(
            jsonb_build_object(
              'id',
              t.source_id,

              'source',
              jsonb_build_object(
                'url',
                (
                  'https://tiles.availabs.org/data/'
                  || t.tileset_name
                  || '.json'
                ),

                'type',
                t.source_type
              )
            )
          )
        ) AS mapbox_config

      FROM (
        SELECT
            a.view_id,
            COALESCE(
              d.mapbox_type,
              c.mapbox_type,
              b.mapbox_type
            ) AS mapbox_type,
            COALESCE(
              d.mapbox_paint_style,
              c.mapbox_paint_style,
              b.mapbox_paint_style
            ) AS mapbox_paint_style,
            (
              (
                COALESCE(
                  d.mapbox_paint_style,
                  c.mapbox_paint_style
                ) IS NULL
              )
              AND
              ( b.mapbox_paint_style IS NOT NULL )
            ) AS is_using_default_mapbox_paint_style,

            e.mbtiles_id,
            e.tileset_timestamp,
            e.tileset_name,
            e.source_id,
            e.source_layer_name,
            e.source_type,
            e.tippecanoe_args,
            e.tippecanoe_filter

          FROM _data_manager_admin.dama_views_comprehensive AS a
            LEFT OUTER JOIN _data_manager_admin.geojson_type_default_mapbox_paint_styles AS b
              USING (geojson_type)
            LEFT OUTER JOIN _data_manager_admin.dama_sources_bespoke_mapbox_paint_style AS c
              USING (source_id)
            LEFT OUTER JOIN _data_manager_admin.dama_views_bespoke_mapbox_paint_style AS d
              USING (view_id)
            LEFT OUTER JOIN (
              SELECT
                  mbtiles_id,
                  view_id,
                  tileset_timestamp,
                  tileset_name,
                  source_id,
                  source_layer_name,
                  source_type,
                  tippecanoe_args,
                  tippecanoe_filter
                FROM (
                  SELECT
                      mbtiles_id,
                      view_id,
                      tileset_timestamp,
                      tileset_name,
                      source_id,
                      source_layer_name,
                      source_type,
                      tippecanoe_args,
                      tippecanoe_filter,
                      row_number() OVER (PARTITION BY view_id ORDER BY tileset_timestamp DESC) AS row_number
                    FROM _data_manager_admin.dama_views_mbtiles_metadata
                ) AS t
                WHERE ( row_number = 1 )
            ) AS e USING (view_id)
  ) AS t
;
