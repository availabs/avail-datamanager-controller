BEGIN ;

UPDATE floodplains.merged_ble
  SET
    fld_zone = 'A',
    zone_subty = NULL
  WHERE ( (string_to_array(gfid, ':'))[3] = 'FP_01PCT' )
;

UPDATE floodplains.merged_ble
  SET
    fld_zone = 'X',
    zone_subty = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
  WHERE ( (string_to_array(gfid, ':'))[3] = 'FP_0_2PCT' )
;

UPDATE floodplains.merged_ble
  SET
    zone_subty = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
  WHERE (
    ( fld_zone = 'X' )
    AND
    ( zone_subty = '0500' )
  )
;

COMMIT ;
