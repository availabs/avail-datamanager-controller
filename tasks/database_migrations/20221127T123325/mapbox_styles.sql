BEGIN ;

INSERT INTO _data_manager_admin.dama_views_bespoke_mapbox_paint_style (
  view_id,
  mapbox_type,
  mapbox_paint_style,
  mapbox_symbology
)
  SELECT
      view_id,
      metadata->'tiles'->'layers'->0->>'type' AS mapbox_type,
      metadata->'tiles'->'layers'->0->'paint' AS mapbox_paint_style,
      metadata->'tiles'->'symbology' AS mapbox_symbology
    FROM data_manager.views
    WHERE (
      ( data_type = 'SPATIAL' )
      AND
      ( ( metadata->'tiles'->'layers'->0->>'type' ) IS NOT NULL )
      AND
      ( ( metadata->'tiles'->'layers'->0->>'paint' ) IS NOT NULL )
    )
;

COMMIT ;
