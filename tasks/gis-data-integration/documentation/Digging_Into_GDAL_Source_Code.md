# Digging_Into_GDAL_Source_Code

- [ogrgeometry.cpp](https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp)
- [GeometryType](https://github.com/OSGeo/gdal/blob/e88d4e82cb1e141e0c96942c386bd6f7af162d8d/ogr/ogrsf_frmts/flatgeobuf/header_generated.h)

- [PGDUMP](https://github.com/OSGeo/gdal/blob/35c07b18316b4b6d238f6d60b82c31e25662ad27/ogr/ogrsf_frmts/pgdump/ogrpgdumpdatasource.cpp)

## [ogr_code.h OGRwkbGeometryType enum](https://github.com/OSGeo/gdal/blob/588a2003da0fffa6049ff433dfdb78dd9ff6b533/ogr/ogr_core.h#L368-L468)

```c++
/**
 * List of well known binary geometry types.  These are used within the BLOBs
 * but are also returned from OGRGeometry::getGeometryType() to identify the
 * type of a geometry object.
 */
typedef enum
{
    wkbUnknown = 0,         /**< unknown type, non-standard */

    wkbPoint = 1,           /**< 0-dimensional geometric object, standard WKB */
    wkbLineString = 2,      /**< 1-dimensional geometric object with linear
                             *   interpolation between Points, standard WKB */
    wkbPolygon = 3,         /**< planar 2-dimensional geometric object defined
                             *   by 1 exterior boundary and 0 or more interior
                             *   boundaries, standard WKB */
    wkbMultiPoint = 4,      /**< GeometryCollection of Points, standard WKB */
    wkbMultiLineString = 5, /**< GeometryCollection of LineStrings, standard WKB */
    wkbMultiPolygon = 6,    /**< GeometryCollection of Polygons, standard WKB */
    wkbGeometryCollection = 7, /**< geometric object that is a collection of 1
                                    or more geometric objects, standard WKB */

    wkbCircularString = 8,  /**< one or more circular arc segments connected end to end,
                             *   ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbCompoundCurve = 9,   /**< sequence of contiguous curves, ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbCurvePolygon = 10,   /**< planar surface, defined by 1 exterior boundary
                             *   and zero or more interior boundaries, that are curves.
                             *    ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbMultiCurve = 11,     /**< GeometryCollection of Curves, ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbMultiSurface = 12,   /**< GeometryCollection of Surfaces, ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbCurve = 13,          /**< Curve (abstract type). ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbSurface = 14,        /**< Surface (abstract type). ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbPolyhedralSurface = 15,/**< a contiguous collection of polygons, which share common boundary segments,
                               *   ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTIN = 16,              /**< a PolyhedralSurface consisting only of Triangle patches
                               *    ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTriangle = 17,         /**< a Triangle. ISO SQL/MM Part 3. GDAL &gt;= 2.3 */

    wkbNone = 100,          /**< non-standard, for pure attribute records */
    wkbLinearRing = 101,    /**< non-standard, just for createGeometry() */

    wkbCircularStringZ = 1008,  /**< wkbCircularString with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbCompoundCurveZ = 1009,   /**< wkbCompoundCurve with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbCurvePolygonZ = 1010,    /**< wkbCurvePolygon with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbMultiCurveZ = 1011,      /**< wkbMultiCurve with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbMultiSurfaceZ = 1012,    /**< wkbMultiSurface with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.0 */
    wkbCurveZ = 1013,           /**< wkbCurve with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbSurfaceZ = 1014,         /**< wkbSurface with Z component. ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbPolyhedralSurfaceZ = 1015,  /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTINZ = 1016,                /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTriangleZ = 1017,           /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */

    wkbPointM = 2001,              /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbLineStringM = 2002,         /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbPolygonM = 2003,            /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiPointM = 2004,         /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiLineStringM = 2005,    /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiPolygonM = 2006,       /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbGeometryCollectionM = 2007, /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCircularStringM = 2008,     /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCompoundCurveM = 2009,      /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCurvePolygonM = 2010,       /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiCurveM = 2011,         /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiSurfaceM = 2012,       /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCurveM = 2013,              /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbSurfaceM = 2014,            /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbPolyhedralSurfaceM = 2015,  /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTINM = 2016,                /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTriangleM = 2017,           /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */

    wkbPointZM = 3001,              /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbLineStringZM = 3002,         /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbPolygonZM = 3003,            /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiPointZM = 3004,         /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiLineStringZM = 3005,    /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiPolygonZM = 3006,       /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbGeometryCollectionZM = 3007, /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCircularStringZM = 3008,     /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCompoundCurveZM = 3009,      /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCurvePolygonZM = 3010,       /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiCurveZM = 3011,         /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbMultiSurfaceZM = 3012,       /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbCurveZM = 3013,              /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbSurfaceZM = 3014,            /**< ISO SQL/MM Part 3. GDAL &gt;= 2.1 */
    wkbPolyhedralSurfaceZM = 3015,  /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTINZM = 3016,                /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */
    wkbTriangleZM = 3017,           /**< ISO SQL/MM Part 3. GDAL &gt;= 2.3 */

#if defined(DOXYGEN_SKIP)
    // Sphinx doesn't like 0x8000000x constants
    wkbPoint25D = -2147483647, /**< 2.5D extension as per 99-402 */
    wkbLineString25D = -2147483646, /**< 2.5D extension as per 99-402 */
    wkbPolygon25D = -2147483645, /**< 2.5D extension as per 99-402 */
    wkbMultiPoint25D = -2147483644, /**< 2.5D extension as per 99-402 */
    wkbMultiLineString25D = -2147483643, /**< 2.5D extension as per 99-402 */
    wkbMultiPolygon25D = -2147483642, /**< 2.5D extension as per 99-402 */
    wkbGeometryCollection25D = -2147483641 /**< 2.5D extension as per 99-402 */
#else
    wkbPoint25D = 0x80000001, /**< 2.5D extension as per 99-402 */
    wkbLineString25D = 0x80000002, /**< 2.5D extension as per 99-402 */
    wkbPolygon25D = 0x80000003, /**< 2.5D extension as per 99-402 */
    wkbMultiPoint25D = 0x80000004, /**< 2.5D extension as per 99-402 */
    wkbMultiLineString25D = 0x80000005, /**< 2.5D extension as per 99-402 */
    wkbMultiPolygon25D = 0x80000006, /**< 2.5D extension as per 99-402 */
    wkbGeometryCollection25D = 0x80000007 /**< 2.5D extension as per 99-402 */
#endif
} OGRwkbGeometryType;
```

## [ogrgeometry.cpp OGRGeometryTypeToName](https://github.com/OSGeo/gdal/blob/915233cf08cbc8b3c23604ad95bcda33a4351ea2/ogr/ogrgeometry.cpp#L2519-L2731)

```c++
/************************************************************************/
/*                       OGRGeometryTypeToName()                        */
/************************************************************************/

/**
 * \brief Fetch a human readable name corresponding to an OGRwkbGeometryType
 * value.  The returned value should not be modified, or freed by the
 * application.
 *
 * This function is C callable.
 *
 * @param eType the geometry type.
 *
 * @return internal human readable string, or NULL on failure.
 */

const char *OGRGeometryTypeToName( OGRwkbGeometryType eType )

{
    bool b3D = wkbHasZ(eType);
    bool bMeasured = wkbHasM(eType);

    switch( wkbFlatten(eType) )
    {
        case wkbUnknown:
            if( b3D && bMeasured )
                return "3D Measured Unknown (any)";
            else if( b3D )
                return "3D Unknown (any)";
            else if( bMeasured )
                return "Measured Unknown (any)";
            else
                return "Unknown (any)";

        case wkbPoint:
            if( b3D && bMeasured )
                return "3D Measured Point";
            else if( b3D )
                return "3D Point";
            else if( bMeasured )
                return "Measured Point";
            else
                return "Point";

        case wkbLineString:
            if( b3D && bMeasured )
                return "3D Measured Line String";
            else if( b3D )
                return "3D Line String";
            else if( bMeasured )
                return "Measured Line String";
            else
                return "Line String";

        case wkbPolygon:
            if( b3D && bMeasured )
                return "3D Measured Polygon";
            else if( b3D )
                return "3D Polygon";
            else if( bMeasured )
                return "Measured Polygon";
            else
                return "Polygon";

        case wkbMultiPoint:
            if( b3D && bMeasured )
                return "3D Measured Multi Point";
            else if( b3D )
                return "3D Multi Point";
            else if( bMeasured )
                return "Measured Multi Point";
            else
                return "Multi Point";

        case wkbMultiLineString:
            if( b3D && bMeasured )
                return "3D Measured Multi Line String";
            else if( b3D )
                return "3D Multi Line String";
            else if( bMeasured )
                return "Measured Multi Line String";
            else
                return "Multi Line String";

        case wkbMultiPolygon:
          if( b3D && bMeasured )
                return "3D Measured Multi Polygon";
            else if( b3D )
                return "3D Multi Polygon";
            else if( bMeasured )
                return "Measured Multi Polygon";
            else
                return "Multi Polygon";

        case wkbGeometryCollection:
            if( b3D && bMeasured )
                return "3D Measured Geometry Collection";
            else if( b3D )
                return "3D Geometry Collection";
            else if( bMeasured )
                return "Measured Geometry Collection";
            else
                return "Geometry Collection";

        case wkbCircularString:
            if( b3D && bMeasured )
                return "3D Measured Circular String";
            else if( b3D )
                return "3D Circular String";
            else if( bMeasured )
                return "Measured Circular String";
            else
                return "Circular String";

        case wkbCompoundCurve:
            if( b3D && bMeasured )
                return "3D Measured Compound Curve";
            else if( b3D )
                return "3D Compound Curve";
            else if( bMeasured )
                return "Measured Compound Curve";
            else
                return "Compound Curve";

        case wkbCurvePolygon:
            if( b3D && bMeasured )
                return "3D Measured Curve Polygon";
            else if( b3D )
                return "3D Curve Polygon";
            else if( bMeasured )
                return "Measured Curve Polygon";
            else
                return "Curve Polygon";

        case wkbMultiCurve:
            if( b3D && bMeasured )
                return "3D Measured Multi Curve";
            else if( b3D )
                return "3D Multi Curve";
            else if( bMeasured )
                return "Measured Multi Curve";
            else
                return "Multi Curve";

        case wkbMultiSurface:
            if( b3D && bMeasured )
                return "3D Measured Multi Surface";
            else if( b3D )
                return "3D Multi Surface";
            else if( bMeasured )
                return "Measured Multi Surface";
            else
                return "Multi Surface";

        case wkbCurve:
            if( b3D && bMeasured )
                return "3D Measured Curve";
            else if( b3D )
                return "3D Curve";
            else if( bMeasured )
                return "Measured Curve";
            else
                return "Curve";

        case wkbSurface:
            if( b3D && bMeasured )
                return "3D Measured Surface";
            else if( b3D )
                return "3D Surface";
            else if( bMeasured )
                return "Measured Surface";
            else
                return "Surface";

        case wkbTriangle:
            if (b3D && bMeasured)
                return "3D Measured Triangle";
            else if (b3D)
                return "3D Triangle";
            else if (bMeasured)
                return "Measured Triangle";
            else
                return "Triangle";

        case wkbPolyhedralSurface:
            if (b3D && bMeasured)
                return "3D Measured PolyhedralSurface";
            else if (b3D)
                return "3D PolyhedralSurface";
            else if (bMeasured)
                return "Measured PolyhedralSurface";
            else
                return "PolyhedralSurface";

        case wkbTIN:
            if (b3D && bMeasured)
                return "3D Measured TIN";
            else if (b3D)
                return "3D TIN";
            else if (bMeasured)
                return "Measured TIN";
            else
                return "TIN";

        case wkbNone:
            return "None";

        default:
        {
            return CPLSPrintf("Unrecognized: %d", static_cast<int>(eType));
        }
    }
}
```

## [ogrgeometry.cpp OGR_GT_HasZ](https://github.com/OSGeo/gdal/blob/master/ogr/ogrgeometry.cpp#L6771-L6793)

```c++
/************************************************************************/
/*                          OGR_GT_HasZ()                               */
/************************************************************************/
/**
 * \brief Return if the geometry type is a 3D geometry type.
 *
 * @param eType Input geometry type
 *
 * @return TRUE if the geometry type is a 3D geometry type.
 *
 * @since GDAL 2.0
 */

int OGR_GT_HasZ( OGRwkbGeometryType eType )
{
    if( eType & wkb25DBitInternalUse )
        return TRUE;
    if( eType >= 1000 && eType < 2000 )  // Accept 1000 for wkbUnknownZ.
        return TRUE;
    if( eType >= 3000 && eType < 4000 )  // Accept 3000 for wkbUnknownZM.
        return TRUE;
    return FALSE;
}
```
