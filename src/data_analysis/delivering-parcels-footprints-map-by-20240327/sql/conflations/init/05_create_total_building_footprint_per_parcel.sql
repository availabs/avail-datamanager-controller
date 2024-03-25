/*
  This file creates the total_building_footprint_per_parcel.
    The total_lcgu_bldg_sq_meters is used to allocate each parcel's
    nonland_av (total_av - land_av) amongst all its associated LCGU parcels
    for which there is an associated building footprint.

    Each building-associtated LCGU gets allocated a portion of the parcel's nonland_av
      proportional to is area relative to the other building-associated LCGUs.

    For example, consider the following:

      Parcel X has a nonland_av of 60.
      Parcel Y has a nonland_av of 40.

      Parcel X is associated with 3 NCGUs:
        A: area 10
        B: area 20
        C: area 30

      Parcel Y is associated with 3 NCGUs:
        B: area 20
        D: area 40

      Only NCGUs A and B are associated with building footprints.
        (It does not matter how may buildings with which either are associated.)

      Parcel X has a total_lcgu_bldg_sq_meters of 30.
      Parcel Y has a total_lcgu_bldg_sq_meters of 20.

      Total nonland_av for each NCGU:

        A: (10/30) * 60) = 20
        B: ((20/30) * 60)) + ((20/20) * 40) = 40 + 40 = 80

      Consider:

          Total nonland_av of parcels X and Y is (60 + 40) = 100.
          Total nonland_av of building-associated LCGUs A and B is (20 + 80) = 100

          We therefore preserve total nonland_av while distributing it across the building-associated LCGUs.

  For each input parcel polygon
    calculate the total NCGU polygon area
    where the input parcel polygon shares the NCGU polygon
      with at least one building footprint.

  NOTE: An input parcel can have many LCGU polys,
        and an LCGU poly can have many input parcels.

        In this table, an LCGU polygon area is included
          in the total_lcgu_bldg_sq_meters FOR EACH of the input parcels
          with which it is associated.

        In otherwords, there is intentional double counting of the LCGU areas.
          Therefore, SUM(total_lcgu_bldg_sq_meters) is greater than
            the sum of all LCGU polygons for which there is an associated building.
*/

BEGIN ;

DROP TABLE IF EXISTS :OUTPUT_SCHEMA.total_building_footprint_per_parcel CASCADE ;
CREATE TABLE :OUTPUT_SCHEMA.total_building_footprint_per_parcel (
  p_ogc_fid                   INTEGER PRIMARY KEY REFERENCES :PARCELS_TABLE_SCHEMA.:PARCELS_TABLE_NAME (ogc_fid),
  p_land_av                   DOUBLE PRECISION,
  p_total_av                  DOUBLE PRECISION,
  total_lcgu_bldg_sq_meters   DOUBLE PRECISION NOT NULL
) WITH (fillfactor=100) ;

INSERT INTO :OUTPUT_SCHEMA.total_building_footprint_per_parcel (
  p_ogc_fid,
  p_land_av,
  p_total_av,
  total_lcgu_bldg_sq_meters
)
  SELECT
      p_ogc_fid,
      -- NOTE: Wrapping p_land_av and p_total_av in MIN rather than group by (p_ogc_fid, p_land_av, p_total_av)
      MIN(p_land_av) AS p_land_av,
      MIN(p_total_av) AS p_total_av,
      SUM(lcgu_area_sq_meters) AS total_lcgu_bldg_sq_meters
    FROM (
      -- Get all distinct NCGU polys for a parcel where there is a building footprint.
      --
      -- Note: (ogc_fid, p_ogc_fid) can occur for many (ogc_fid, n_ogc_fid) pairs
      --       therefore we must use DISTINT ON
      SELECT DISTINCT ON (ogc_fid, p_ogc_fid)
          ogc_fid,
          lcgu_area_sq_meters,
          p_ogc_fid,
          p_land_av,
          p_total_av
        FROM :OUTPUT_SCHEMA.lcgu_poly_ancestor_properties
        WHERE (
          ( p_ogc_fid IS NOT NULL )
          AND
          ( n_ogc_fid IS NOT NULL )
        )
        ORDER BY ogc_fid, p_ogc_fid
    ) AS t
    GROUP BY 1
;

CLUSTER :OUTPUT_SCHEMA.total_building_footprint_per_parcel
  USING total_building_footprint_per_parcel_pkey
;

COMMIT ;
