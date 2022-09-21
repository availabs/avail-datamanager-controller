/******************************************************************************
 *
 * Project:  OpenGIS Simple Features Reference Implementation
 * Purpose:  Implements a few base methods on OGRGeometry.
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 1999, Frank Warmerdam
 * Copyright (c) 2008-2013, Even Rouault <even dot rouault at spatialys.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

import _ from "lodash";

const wkb25DBitInternalUse = 0x80000000;

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogr_core.h
export enum OGRwkbGeometryType {
  wkbUnknown = 0,

  wkbPoint = 1,
  wkbLineString = 2,
  wkbPolygon = 3,
  wkbMultiPoint = 4,
  wkbMultiLineString = 5,
  wkbMultiPolygon = 6,
  wkbGeometryCollection = 7,

  wkbCircularString = 8,
  wkbCompoundCurve = 9,
  wkbCurvePolygon = 10,
  wkbMultiCurve = 11,
  wkbMultiSurface = 12,
  wkbCurve = 13,
  wkbSurface = 14,
  wkbPolyhedralSurface = 15,
  wkbTIN = 16,
  wkbTriangle = 17,

  wkbNone = 100,
  wkbLinearRing = 101,

  wkbCircularStringZ = 1008,
  wkbCompoundCurveZ = 1009,
  wkbCurvePolygonZ = 1010,
  wkbMultiCurveZ = 1011,
  wkbMultiSurfaceZ = 1012,
  wkbCurveZ = 1013,
  wkbSurfaceZ = 1014,
  wkbPolyhedralSurfaceZ = 1015,
  wkbTINZ = 1016,
  wkbTriangleZ = 1017,

  wkbPointM = 2001,
  wkbLineStringM = 2002,
  wkbPolygonM = 2003,
  wkbMultiPointM = 2004,
  wkbMultiLineStringM = 2005,
  wkbMultiPolygonM = 2006,
  wkbGeometryCollectionM = 2007,
  wkbCircularStringM = 2008,
  wkbCompoundCurveM = 2009,
  wkbCurvePolygonM = 2010,
  wkbMultiCurveM = 2011,
  wkbMultiSurfaceM = 2012,
  wkbCurveM = 2013,
  wkbSurfaceM = 2014,
  wkbPolyhedralSurfaceM = 2015,
  wkbTINM = 2016,
  wkbTriangleM = 2017,

  wkbPointZM = 3001,
  wkbLineStringZM = 3002,
  wkbPolygonZM = 3003,
  wkbMultiPointZM = 3004,
  wkbMultiLineStringZM = 3005,
  wkbMultiPolygonZM = 3006,
  wkbGeometryCollectionZM = 3007,
  wkbCircularStringZM = 3008,
  wkbCompoundCurveZM = 3009,
  wkbCurvePolygonZM = 3010,
  wkbMultiCurveZM = 3011,
  wkbMultiSurfaceZM = 3012,
  wkbCurveZM = 3013,
  wkbSurfaceZM = 3014,
  wkbPolyhedralSurfaceZM = 3015,
  wkbTINZM = 3016,
  wkbTriangleZM = 3017,

  wkbPoint25D = -2147483647,
  wkbLineString25D = -2147483646,
  wkbPolygon25D = -2147483645,
  wkbMultiPoint25D = -2147483644,
  wkbMultiLineString25D = -2147483643,
  wkbMultiPolygon25D = -2147483642,
  wkbGeometryCollection25D = -2147483641,
}

export const OGRwkbGeometryCodeToType = _.invert(OGRwkbGeometryType);

// https://github.com/OSGeo/gdal/blob/master/ogr/ogrgeometry.cpp#L6771-L6793
export function OGR_GT_HasZ(eType: OGRwkbGeometryType) {
  if (eType >= 1000 && eType < 2000) {
    // Accept 1000 for wkbUnknownZ.
    return true;
  }
  if (eType >= 3000 && eType < 4000) {
    // Accept 3000 for wkbUnknownZM.
    return true;
  }
  return false;
}

// https://github.com/OSGeo/gdal/blob/master/ogr/ogr_core.h#L507
export const wkbHasZ = OGR_GT_HasZ;

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L6817-L6838
export function OGR_GT_SetZ(eType: OGRwkbGeometryType) {
  if (OGR_GT_HasZ(eType) || eType == OGRwkbGeometryType.wkbNone) return eType;

  if (eType <= OGRwkbGeometryType.wkbGeometryCollection)
    return <OGRwkbGeometryType>(eType | wkb25DBitInternalUse);
  else return <OGRwkbGeometryType>(eType + 1000);
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogr_core.h#L512
export const wkbSetZ = OGR_GT_SetZ;

// https://github.com/OSGeo/gdal/blob/master/ogr/ogrgeometry.cpp#L6795-L6815
export function OGR_GT_HasM(eType: OGRwkbGeometryType) {
  if (eType >= 2000 && eType < 3000)
    // Accept 2000 for wkbUnknownM.
    return true;
  if (eType >= 3000 && eType < 4000)
    // Accept 3000 for wkbUnknownZM.
    return true;
  return false;
}

// https://github.com/OSGeo/gdal/blob/master/ogr/ogr_core.h#L517
export const wkbHasM = OGR_GT_HasM;

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L6840-L6863
export function OGR_GT_SetM(eType: OGRwkbGeometryType) {
  if (OGR_GT_HasM(eType) || eType == OGRwkbGeometryType.wkbNone) return eType;
  if (eType & wkb25DBitInternalUse) {
    eType = <OGRwkbGeometryType>(eType & ~wkb25DBitInternalUse);
    eType = <OGRwkbGeometryType>(eType + 1000);
  }
  return <OGRwkbGeometryType>(eType + 2000);
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogr_core.h#L522
export const wkbSetM = OGR_GT_SetM;

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L6741-L6769
export function OGR_GT_Flatten(eType: OGRwkbGeometryType): OGRwkbGeometryType {
  eType = <OGRwkbGeometryType>(eType & ~wkb25DBitInternalUse);
  if (eType >= 1000 && eType < 2000)
    // ISO Z.
    return <OGRwkbGeometryType>(eType - 1000);
  if (eType >= 2000 && eType < 3000)
    // ISO M.
    return <OGRwkbGeometryType>(eType - 2000);
  if (eType >= 3000 && eType < 4000)
    // ISO ZM.
    return <OGRwkbGeometryType>(eType - 3000);
  return eType;
}

// https://github.com/OSGeo/gdal/blob/master/ogr/ogr_core.h#L501
export const wkbFlatten = OGR_GT_Flatten;

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L6893-L6946
export function OGR_GT_IsSubClassOf(
  eType: OGRwkbGeometryType,
  eSuperType: OGRwkbGeometryType
) {
  eSuperType = wkbFlatten(eSuperType);
  eType = wkbFlatten(eType);

  if (eSuperType == eType || eSuperType == OGRwkbGeometryType.wkbUnknown)
    return true;

  if (eSuperType == OGRwkbGeometryType.wkbGeometryCollection)
    return (
      eType == OGRwkbGeometryType.wkbMultiPoint ||
      eType == OGRwkbGeometryType.wkbMultiLineString ||
      eType == OGRwkbGeometryType.wkbMultiPolygon ||
      eType == OGRwkbGeometryType.wkbMultiCurve ||
      eType == OGRwkbGeometryType.wkbMultiSurface
    );

  if (eSuperType == OGRwkbGeometryType.wkbCurvePolygon)
    return (
      eType == OGRwkbGeometryType.wkbPolygon ||
      eType == OGRwkbGeometryType.wkbTriangle
    );

  if (eSuperType == OGRwkbGeometryType.wkbMultiCurve)
    return eType == OGRwkbGeometryType.wkbMultiLineString;

  if (eSuperType == OGRwkbGeometryType.wkbMultiSurface)
    return eType == OGRwkbGeometryType.wkbMultiPolygon;

  if (eSuperType == OGRwkbGeometryType.wkbCurve)
    return (
      eType == OGRwkbGeometryType.wkbLineString ||
      eType == OGRwkbGeometryType.wkbCircularString ||
      eType == OGRwkbGeometryType.wkbCompoundCurve
    );

  if (eSuperType == OGRwkbGeometryType.wkbSurface)
    return (
      eType == OGRwkbGeometryType.wkbCurvePolygon ||
      eType == OGRwkbGeometryType.wkbPolygon ||
      eType == OGRwkbGeometryType.wkbTriangle ||
      eType == OGRwkbGeometryType.wkbPolyhedralSurface ||
      eType == OGRwkbGeometryType.wkbTIN
    );

  if (eSuperType == OGRwkbGeometryType.wkbPolygon)
    return eType == OGRwkbGeometryType.wkbTriangle;

  if (eSuperType == OGRwkbGeometryType.wkbPolyhedralSurface)
    return eType == OGRwkbGeometryType.wkbTIN;

  return false;
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L7123-L7142
export function OGR_GT_IsSurface(eGeomType: OGRwkbGeometryType) {
  return OGR_GT_IsSubClassOf(eGeomType, OGRwkbGeometryType.wkbSurface);
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L7102-L7121
export function OGR_GT_IsCurve(eGeomType: OGRwkbGeometryType) {
  return OGR_GT_IsSubClassOf(eGeomType, OGRwkbGeometryType.wkbCurve);
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L2519-L2731
export function OGRGeometryTypeToName(eType: OGRwkbGeometryType) {
  const b3D = wkbHasZ(eType);
  const bMeasured = wkbHasM(eType);

  switch (wkbFlatten(eType)) {
    case OGRwkbGeometryType.wkbUnknown:
      if (b3D && bMeasured) return "3D Measured Unknown (any)";
      else if (b3D) return "3D Unknown (any)";
      else if (bMeasured) return "Measured Unknown (any)";
      else return "Unknown (any)";

    case OGRwkbGeometryType.wkbPoint:
      if (b3D && bMeasured) return "3D Measured Point";
      else if (b3D) return "3D Point";
      else if (bMeasured) return "Measured Point";
      else return "Point";

    case OGRwkbGeometryType.wkbLineString:
      if (b3D && bMeasured) return "3D Measured Line String";
      else if (b3D) return "3D Line String";
      else if (bMeasured) return "Measured Line String";
      else return "Line String";

    case OGRwkbGeometryType.wkbPolygon:
      if (b3D && bMeasured) return "3D Measured Polygon";
      else if (b3D) return "3D Polygon";
      else if (bMeasured) return "Measured Polygon";
      else return "Polygon";

    case OGRwkbGeometryType.wkbMultiPoint:
      if (b3D && bMeasured) return "3D Measured Multi Point";
      else if (b3D) return "3D Multi Point";
      else if (bMeasured) return "Measured Multi Point";
      else return "Multi Point";

    case OGRwkbGeometryType.wkbMultiLineString:
      if (b3D && bMeasured) return "3D Measured Multi Line String";
      else if (b3D) return "3D Multi Line String";
      else if (bMeasured) return "Measured Multi Line String";
      else return "Multi Line String";

    case OGRwkbGeometryType.wkbMultiPolygon:
      if (b3D && bMeasured) return "3D Measured Multi Polygon";
      else if (b3D) return "3D Multi Polygon";
      else if (bMeasured) return "Measured Multi Polygon";
      else return "Multi Polygon";

    case OGRwkbGeometryType.wkbGeometryCollection:
      if (b3D && bMeasured) return "3D Measured Geometry Collection";
      else if (b3D) return "3D Geometry Collection";
      else if (bMeasured) return "Measured Geometry Collection";
      else return "Geometry Collection";

    case OGRwkbGeometryType.wkbCircularString:
      if (b3D && bMeasured) return "3D Measured Circular String";
      else if (b3D) return "3D Circular String";
      else if (bMeasured) return "Measured Circular String";
      else return "Circular String";

    case OGRwkbGeometryType.wkbCompoundCurve:
      if (b3D && bMeasured) return "3D Measured Compound Curve";
      else if (b3D) return "3D Compound Curve";
      else if (bMeasured) return "Measured Compound Curve";
      else return "Compound Curve";

    case OGRwkbGeometryType.wkbCurvePolygon:
      if (b3D && bMeasured) return "3D Measured Curve Polygon";
      else if (b3D) return "3D Curve Polygon";
      else if (bMeasured) return "Measured Curve Polygon";
      else return "Curve Polygon";

    case OGRwkbGeometryType.wkbMultiCurve:
      if (b3D && bMeasured) return "3D Measured Multi Curve";
      else if (b3D) return "3D Multi Curve";
      else if (bMeasured) return "Measured Multi Curve";
      else return "Multi Curve";

    case OGRwkbGeometryType.wkbMultiSurface:
      if (b3D && bMeasured) return "3D Measured Multi Surface";
      else if (b3D) return "3D Multi Surface";
      else if (bMeasured) return "Measured Multi Surface";
      else return "Multi Surface";

    case OGRwkbGeometryType.wkbCurve:
      if (b3D && bMeasured) return "3D Measured Curve";
      else if (b3D) return "3D Curve";
      else if (bMeasured) return "Measured Curve";
      else return "Curve";

    case OGRwkbGeometryType.wkbSurface:
      if (b3D && bMeasured) return "3D Measured Surface";
      else if (b3D) return "3D Surface";
      else if (bMeasured) return "Measured Surface";
      else return "Surface";

    case OGRwkbGeometryType.wkbTriangle:
      if (b3D && bMeasured) return "3D Measured Triangle";
      else if (b3D) return "3D Triangle";
      else if (bMeasured) return "Measured Triangle";
      else return "Triangle";

    case OGRwkbGeometryType.wkbPolyhedralSurface:
      if (b3D && bMeasured) return "3D Measured PolyhedralSurface";
      else if (b3D) return "3D PolyhedralSurface";
      else if (bMeasured) return "Measured PolyhedralSurface";
      else return "PolyhedralSurface";

    case OGRwkbGeometryType.wkbTIN:
      if (b3D && bMeasured) return "3D Measured TIN";
      else if (b3D) return "3D TIN";
      else if (bMeasured) return "Measured TIN";
      else return "TIN";

    case OGRwkbGeometryType.wkbNone:
      return "None";

    default: {
      return "Unrecognized OGRwkbGeometryType";
    }
  }
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L6948-L7005
export function OGR_GT_GetCollection(eType: OGRwkbGeometryType) {
  const bHasZ = wkbHasZ(eType);
  const bHasM = wkbHasM(eType);

  if (eType == OGRwkbGeometryType.wkbNone) return OGRwkbGeometryType.wkbNone;

  const eFGType = wkbFlatten(eType);

  const _eType = eType;

  if (eFGType == OGRwkbGeometryType.wkbPoint)
    eType = OGRwkbGeometryType.wkbMultiPoint;
  else if (eFGType == OGRwkbGeometryType.wkbLineString)
    eType = OGRwkbGeometryType.wkbMultiLineString;
  else if (eFGType == OGRwkbGeometryType.wkbPolygon)
    eType = OGRwkbGeometryType.wkbMultiPolygon;
  else if (eFGType == OGRwkbGeometryType.wkbTriangle)
    eType = OGRwkbGeometryType.wkbTIN;
  else if (OGR_GT_IsCurve(eFGType)) eType = OGRwkbGeometryType.wkbMultiCurve;
  else if (OGR_GT_IsSurface(eFGType))
    eType = OGRwkbGeometryType.wkbMultiSurface;
  else return OGRwkbGeometryType.wkbUnknown;

  if (bHasZ) eType = wkbSetZ(eType);
  if (bHasM) eType = wkbSetM(eType);

  console.log("OGR_GT_GetCollection:", _eType, eFGType, eType);
  return eType;
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L6865-L6891
export function OGR_GT_SetModifier(
  eType: OGRwkbGeometryType,
  bHasZ: boolean,
  bHasM: boolean
) {
  if (bHasZ && bHasM) return OGR_GT_SetM(OGR_GT_SetZ(eType));
  else if (bHasM) return OGR_GT_SetM(wkbFlatten(eType));
  else if (bHasZ) return OGR_GT_SetZ(wkbFlatten(eType));
  else return wkbFlatten(eType);
}

// https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L2733-L2851
export function OGRMergeGeometryTypes(
  eMain: OGRwkbGeometryType,
  eExtra: OGRwkbGeometryType,
  bAllowPromotingToCurves: boolean = false
) {
  const eFMain = wkbFlatten(eMain);
  const eFExtra = wkbFlatten(eExtra);

  const bHasZ = wkbHasZ(eMain) || wkbHasZ(eExtra);
  const bHasM = wkbHasM(eMain) || wkbHasM(eExtra);

  if (
    eFMain == OGRwkbGeometryType.wkbUnknown ||
    eFExtra == OGRwkbGeometryType.wkbUnknown
  )
    return OGR_GT_SetModifier(OGRwkbGeometryType.wkbUnknown, bHasZ, bHasM);

  if (eFMain == OGRwkbGeometryType.wkbNone) return eExtra;

  if (eFExtra == OGRwkbGeometryType.wkbNone) return eMain;

  if (eFMain == eFExtra) {
    return OGR_GT_SetModifier(eFMain, bHasZ, bHasM);
  }

  if (bAllowPromotingToCurves) {
    if (OGR_GT_IsCurve(eFMain) && OGR_GT_IsCurve(eFExtra))
      return OGR_GT_SetModifier(
        OGRwkbGeometryType.wkbCompoundCurve,
        bHasZ,
        bHasM
      );

    if (OGR_GT_IsSubClassOf(eFMain, eFExtra))
      return OGR_GT_SetModifier(eFExtra, bHasZ, bHasM);

    if (OGR_GT_IsSubClassOf(eFExtra, eFMain))
      return OGR_GT_SetModifier(eFMain, bHasZ, bHasM);
  }

  // Both are geometry collections.
  if (
    OGR_GT_IsSubClassOf(eFMain, OGRwkbGeometryType.wkbGeometryCollection) &&
    OGR_GT_IsSubClassOf(eFExtra, OGRwkbGeometryType.wkbGeometryCollection)
  ) {
    return OGR_GT_SetModifier(
      OGRwkbGeometryType.wkbGeometryCollection,
      bHasZ,
      bHasM
    );
  }

  // One is subclass of the other one
  if (OGR_GT_IsSubClassOf(eFMain, eFExtra)) {
    return OGR_GT_SetModifier(eFExtra, bHasZ, bHasM);
  } else if (OGR_GT_IsSubClassOf(eFExtra, eFMain)) {
    return OGR_GT_SetModifier(eFMain, bHasZ, bHasM);
  }

  // Nothing apparently in common.
  return OGR_GT_SetModifier(OGRwkbGeometryType.wkbUnknown, bHasZ, bHasM);
}

export const OGRwkbGeometryNameToTypeName = Object.keys(
  OGRwkbGeometryType
).reduce((acc, eTypeName) => {
  const name = OGRGeometryTypeToName(OGRwkbGeometryType[eTypeName]);

  acc[name] = acc[name] || eTypeName;

  return acc;
}, {});

export function simplifyOGRwkbGeometryType(eType: OGRwkbGeometryType) {
  const name = OGRGeometryTypeToName(eType);
  const sTypeName = OGRwkbGeometryNameToTypeName[name];
  const sType = OGRwkbGeometryType[sTypeName] || OGRwkbGeometryType.wkbUnknown;

  return <OGRwkbGeometryType>sType;
}
