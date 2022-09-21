import _ from "lodash";

import { OGRwkbGeometryType, wkbHasM, wkbHasZ } from "./OGRwkbGeometryType";

// From https://github.com/postgis/postgis/blob/master/regress/core/postgis_type_name_expected
export enum PostGisGeometryTypes {
  Geometry = "Geometry",
  GeometryZ = "GeometryZ",
  GeometryZM = "GeometryZM",
  GeometryM = "GeometryM",

  Point = "Point",
  PointZ = "PointZ",
  PointZM = "PointZM",
  PointM = "PointM",

  LineString = "LineString",
  LineStringZ = "LineStringZ",
  LineStringZM = "LineStringZM",
  LineStringM = "LineStringM",

  Polygon = "Polygon",
  PolygonZ = "PolygonZ",
  PolygonZM = "PolygonZM",
  PolygonM = "PolygonM",

  MultiPoint = "MultiPoint",
  MultiPointZ = "MultiPointZ",
  MultiPointZM = "MultiPointZM",
  MultiPointM = "MultiPointM",

  MultiLineString = "MultiLineString",
  MultiLineStringZ = "MultiLineStringZ",
  MultiLineStringZM = "MultiLineStringZM",
  MultiLineStringM = "MultiLineStringM",

  MultiPolygon = "MultiPolygon",
  MultiPolygonZ = "MultiPolygonZ",
  MultiPolygonZM = "MultiPolygonZM",
  MultiPolygonM = "MultiPolygonM",

  GeometryCollection = "GeometryCollection",
  GeometryCollectionZ = "GeometryCollectionZ",
  GeometryCollectionZM = "GeometryCollectionZM",
  GeometryCollectionM = "GeometryCollectionM",

  CircularString = "CircularString",
  CircularStringZ = "CircularStringZ",
  CircularStringZM = "CircularStringZM",
  CircularStringM = "CircularStringM",

  CompoundCurve = "CompoundCurve",
  CompoundCurveZ = "CompoundCurveZ",
  CompoundCurveZM = "CompoundCurveZM",
  CompoundCurveM = "CompoundCurveM",

  CurvePolygon = "CurvePolygon",
  CurvePolygonZ = "CurvePolygonZ",
  CurvePolygonZM = "CurvePolygonZM",
  CurvePolygonM = "CurvePolygonM",

  MultiCurve = "MultiCurve",
  MultiCurveZ = "MultiCurveZ",
  MultiCurveZM = "MultiCurveZM",
  MultiCurveM = "MultiCurveM",

  MultiSurface = "MultiSurface",
  MultiSurfaceZ = "MultiSurfaceZ",
  MultiSurfaceZM = "MultiSurfaceZM",
  MultiSurfaceM = "MultiSurfaceM",

  PolyhedralSurface = "PolyhedralSurface",
  PolyhedralSurfaceZ = "PolyhedralSurfaceZ",
  PolyhedralSurfaceZM = "PolyhedralSurfaceZM",
  PolyhedralSurfaceM = "PolyhedralSurfaceM",

  Triangle = "Triangle",
  TriangleZ = "TriangleZ",
  TriangleZM = "TriangleZM",
  TriangleM = "TriangleM",

  Tin = "Tin",
  TinZ = "TinZ",
  TinZM = "TinZM",
  TinM = "TinM",
}

export const OGRwkbGeometryTypeToPostGisType = {
  [OGRwkbGeometryType.wkbUnknown]: PostGisGeometryTypes.Geometry,

  [OGRwkbGeometryType.wkbPoint]: PostGisGeometryTypes.Point,
  [OGRwkbGeometryType.wkbLineString]: PostGisGeometryTypes.LineString,
  [OGRwkbGeometryType.wkbPolygon]: PostGisGeometryTypes.Polygon,
  [OGRwkbGeometryType.wkbMultiPoint]: PostGisGeometryTypes.MultiPoint,
  [OGRwkbGeometryType.wkbMultiLineString]: PostGisGeometryTypes.MultiLineString,
  [OGRwkbGeometryType.wkbMultiPolygon]: PostGisGeometryTypes.MultiPolygon,
  [OGRwkbGeometryType.wkbGeometryCollection]:
    PostGisGeometryTypes.GeometryCollection,

  [OGRwkbGeometryType.wkbCircularString]: PostGisGeometryTypes.CircularString,
  [OGRwkbGeometryType.wkbCompoundCurve]: PostGisGeometryTypes.CompoundCurve,
  [OGRwkbGeometryType.wkbCurvePolygon]: PostGisGeometryTypes.CurvePolygon,
  [OGRwkbGeometryType.wkbMultiCurve]: PostGisGeometryTypes.MultiCurve,
  [OGRwkbGeometryType.wkbMultiSurface]: PostGisGeometryTypes.MultiSurface,
  // [OGRwkbGeometryType.wkbCurve]: PostGisGeometryTypes.Curve,
  // [OGRwkbGeometryType.wkbSurface]: PostGisGeometryTypes.Surface,
  [OGRwkbGeometryType.wkbPolyhedralSurface]:
    PostGisGeometryTypes.PolyhedralSurface,
  [OGRwkbGeometryType.wkbTIN]: PostGisGeometryTypes.Tin,
  [OGRwkbGeometryType.wkbTriangle]: PostGisGeometryTypes.Triangle,

  // [OGRwkbGeometryType.wkbNone]: PostGisGeometryTypes.None,
  // [OGRwkbGeometryType.wkbLinearRing]: PostGisGeometryTypes.LinearRing,

  [OGRwkbGeometryType.wkbCircularStringZ]: PostGisGeometryTypes.CircularStringZ,
  [OGRwkbGeometryType.wkbCompoundCurveZ]: PostGisGeometryTypes.CompoundCurveZ,
  [OGRwkbGeometryType.wkbCurvePolygonZ]: PostGisGeometryTypes.CurvePolygonZ,
  [OGRwkbGeometryType.wkbMultiCurveZ]: PostGisGeometryTypes.MultiCurveZ,
  [OGRwkbGeometryType.wkbMultiSurfaceZ]: PostGisGeometryTypes.MultiSurfaceZ,
  // [OGRwkbGeometryType.wkbCurveZ]: PostGisGeometryTypes.CurveZ,
  // [OGRwkbGeometryType.wkbSurfaceZ]: PostGisGeometryTypes.SurfaceZ,
  [OGRwkbGeometryType.wkbPolyhedralSurfaceZ]:
    PostGisGeometryTypes.PolyhedralSurfaceZ,
  [OGRwkbGeometryType.wkbTINZ]: PostGisGeometryTypes.TinZ,
  [OGRwkbGeometryType.wkbTriangleZ]: PostGisGeometryTypes.TriangleZ,

  [OGRwkbGeometryType.wkbPointM]: PostGisGeometryTypes.PointM,
  [OGRwkbGeometryType.wkbLineStringM]: PostGisGeometryTypes.LineStringM,
  [OGRwkbGeometryType.wkbPolygonM]: PostGisGeometryTypes.PolygonM,
  [OGRwkbGeometryType.wkbMultiPointM]: PostGisGeometryTypes.MultiPointM,
  [OGRwkbGeometryType.wkbMultiLineStringM]:
    PostGisGeometryTypes.MultiLineStringM,
  [OGRwkbGeometryType.wkbMultiPolygonM]: PostGisGeometryTypes.MultiPolygonM,
  [OGRwkbGeometryType.wkbGeometryCollectionM]:
    PostGisGeometryTypes.GeometryCollectionM,
  [OGRwkbGeometryType.wkbCircularStringM]: PostGisGeometryTypes.CircularStringM,
  [OGRwkbGeometryType.wkbCompoundCurveM]: PostGisGeometryTypes.CompoundCurveM,
  [OGRwkbGeometryType.wkbCurvePolygonM]: PostGisGeometryTypes.CurvePolygonM,
  [OGRwkbGeometryType.wkbMultiCurveM]: PostGisGeometryTypes.MultiCurveM,
  [OGRwkbGeometryType.wkbMultiSurfaceM]: PostGisGeometryTypes.MultiSurfaceM,
  // [OGRwkbGeometryType.wkbCurveM]: PostGisGeometryTypes.CurveM,
  // [OGRwkbGeometryType.wkbSurfaceM]: PostGisGeometryTypes.SurfaceM,
  [OGRwkbGeometryType.wkbPolyhedralSurfaceM]:
    PostGisGeometryTypes.PolyhedralSurfaceM,
  [OGRwkbGeometryType.wkbTINM]: PostGisGeometryTypes.TinM,
  [OGRwkbGeometryType.wkbTriangleM]: PostGisGeometryTypes.TriangleM,

  [OGRwkbGeometryType.wkbPointZM]: PostGisGeometryTypes.PointZM,
  [OGRwkbGeometryType.wkbLineStringZM]: PostGisGeometryTypes.LineStringZM,
  [OGRwkbGeometryType.wkbPolygonZM]: PostGisGeometryTypes.PolygonZM,
  [OGRwkbGeometryType.wkbMultiPointZM]: PostGisGeometryTypes.MultiPointZM,
  [OGRwkbGeometryType.wkbMultiLineStringZM]:
    PostGisGeometryTypes.MultiLineStringZM,
  [OGRwkbGeometryType.wkbMultiPolygonZM]: PostGisGeometryTypes.MultiPolygonZM,
  [OGRwkbGeometryType.wkbGeometryCollectionZM]:
    PostGisGeometryTypes.GeometryCollectionZM,
  [OGRwkbGeometryType.wkbCircularStringZM]:
    PostGisGeometryTypes.CircularStringZM,
  [OGRwkbGeometryType.wkbCompoundCurveZM]: PostGisGeometryTypes.CompoundCurveZM,
  [OGRwkbGeometryType.wkbCurvePolygonZM]: PostGisGeometryTypes.CurvePolygonZM,
  [OGRwkbGeometryType.wkbMultiCurveZM]: PostGisGeometryTypes.MultiCurveZM,
  [OGRwkbGeometryType.wkbMultiSurfaceZM]: PostGisGeometryTypes.MultiSurfaceZM,
  // [OGRwkbGeometryType.wkbCurveZM]: PostGisGeometryTypes.CurveZM,
  // [OGRwkbGeometryType.wkbSurfaceZM]: PostGisGeometryTypes.SurfaceZM,
  [OGRwkbGeometryType.wkbPolyhedralSurfaceZM]:
    PostGisGeometryTypes.PolyhedralSurfaceZM,
  [OGRwkbGeometryType.wkbTINZM]: PostGisGeometryTypes.TinZM,
  [OGRwkbGeometryType.wkbTriangleZM]: PostGisGeometryTypes.TriangleZM,

  // ASSUMPTION: Not sure about the 25D ones
  [OGRwkbGeometryType.wkbPoint25D]: PostGisGeometryTypes.PointZ,
  [OGRwkbGeometryType.wkbLineString25D]: PostGisGeometryTypes.LineStringZ,
  [OGRwkbGeometryType.wkbPolygon25D]: PostGisGeometryTypes.PolygonZ,
  [OGRwkbGeometryType.wkbMultiPoint25D]: PostGisGeometryTypes.MultiPointZ,
  [OGRwkbGeometryType.wkbMultiLineString25D]:
    PostGisGeometryTypes.MultiLineStringZ,
  [OGRwkbGeometryType.wkbMultiPolygon25D]: PostGisGeometryTypes.MultiPolygonZ,
  [OGRwkbGeometryType.wkbGeometryCollection25D]:
    PostGisGeometryTypes.GeometryCollectionZ,
};

export const PostGisTypeToOGRwkbGeometryType = _.invert(PostGisGeometryTypes);

// For coordinateDimensions,
//   see https://github.com/postgis/postgis/blob/master/postgis/postgis.sql.in
// For ogr2ogr PostgreSQL Driver Dimensions,
//   see https://gdal.org/drivers/vector/pg.html#layer-creation-options
export enum PostGisDimension {
  XY = "XY",
  XYZ = "XYZ",
  XYZM = "XYZM",
  XYM = "XYM",
}

export function getPostGisDimensionForOGRwkbGeometryType(
  eType: OGRwkbGeometryType
) {
  const is3d = wkbHasZ(eType);
  const isMeasured = wkbHasM(eType);

  if (is3d && isMeasured) {
    return PostGisDimension.XYZM;
  } else if (is3d) {
    return PostGisDimension.XYZ;
  } else if (isMeasured) {
    return PostGisDimension.XYM;
  }

  return PostGisDimension.XY;
}
