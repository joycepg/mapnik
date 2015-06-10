/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * adapted from ogrmssqlgeometryparser.cpp, part of OGR
 * original copyright notice follows
 *
 *****************************************************************************/


/******************************************************************************
 * $Id: ogrmssqlgeometryparser.cpp 24918 2012-09-07 12:02:01Z tamas $
 *
 * Project:  MSSQL Spatial driver
 * Purpose:  Implements ogrmssqlgeometryparser class to parse native SqlGeometries.
 * Author:   Tamas Szekeres, szekerest at gmail.com
 *
 ******************************************************************************
 * Copyright (c) 2010, Tamas Szekeres
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

#include "sqlserver_geometry_parser.hpp"

#include <mapnik/datasource.hpp>

class sqlserver_geometry_parser_exception : public sqlserver_datasource_exception
{
public:
    sqlserver_geometry_parser_exception(std::string const& message)
    : sqlserver_datasource_exception("Geometry Parser: "+message) {}
    
    virtual ~sqlserver_geometry_parser_exception() throw() {}
};


/*   SqlGeometry serialization format

Simple Point (SerializationProps & IsSinglePoint)
  [SRID][0x01][SerializationProps][Point][z][m]

Simple Line Segment (SerializationProps & IsSingleLineSegment)
  [SRID][0x01][SerializationProps][Point1][Point2][z1][z2][m1][m2]

Complex Geometries
  [SRID][0x01][SerializationProps][NumPoints][Point1]..[PointN][z1]..[zN][m1]..[mN]
  [NumFigures][Figure]..[Figure][NumShapes][Shape]..[Shape]

SRID
  Spatial Reference Id (4 bytes)

SerializationProps (bitmask) 1 byte
  0x01 = HasZValues
  0x02 = HasMValues
  0x04 = IsValid
  0x08 = IsSinglePoint
  0x10 = IsSingleLineSegment
  0x20 = IsWholeGlobe

Point (2-4)x8 bytes, size depends on SerializationProps & HasZValues & HasMValues
  [x][y]                  - SqlGeometry
  [latitude][longitude]   - SqlGeography

Figure
  [FigureAttribute][PointOffset]

FigureAttribute (1 byte)
  0x00 = Interior Ring
  0x01 = Stroke
  0x02 = Exterior Ring

Shape
  [ParentFigureOffset][FigureOffset][ShapeType]

ShapeType (1 byte)
  0x00 = Unknown
  0x01 = Point
  0x02 = LineString
  0x03 = Polygon
  0x04 = MultiPoint
  0x05 = MultiLineString
  0x06 = MultiPolygon
  0x07 = GeometryCollection

*/

/************************************************************************/
/*                         Geometry parser macros                       */
/************************************************************************/

#define SP_NONE 0
#define SP_HASZVALUES 1
#define SP_HASMVALUES 2
#define SP_ISVALID 4
#define SP_ISSINGLEPOINT 8
#define SP_ISSINGLELINESEGMENT 0x10
#define SP_ISWHOLEGLOBE 0x20

#define ST_UNKNOWN 0
#define ST_POINT 1
#define ST_LINESTRING 2
#define ST_POLYGON 3
#define ST_MULTIPOINT 4
#define ST_MULTILINESTRING 5
#define ST_MULTIPOLYGON 6
#define ST_GEOMETRYCOLLECTION 7

#define ReadInt32(nPos) (*((unsigned int*)(pszData + (nPos))))

#define ReadByte(nPos) (pszData[nPos])

#define ReadDouble(nPos) (*((double*)(pszData + (nPos))))

#define ParentOffset(iShape) (ReadInt32(nShapePos + (iShape) * 9 ))
#define FigureOffset(iShape) (ReadInt32(nShapePos + (iShape) * 9 + 4))
#define ShapeType(iShape) (ReadByte(nShapePos + (iShape) * 9 + 8))

#define NextFigureOffset(iShape) (iShape + 1 < nNumShapes? FigureOffset((iShape) +1) : nNumFigures)

#define FigureAttribute(iFigure) (ReadByte(nFigurePos + (iFigure) * 5))
#define PointOffset(iFigure) (ReadInt32(nFigurePos + (iFigure) * 5 + 1))
#define NextPointOffset(iFigure) (iFigure + 1 < nNumFigures? PointOffset((iFigure) +1) : nNumPoints)

#define ReadX(iPoint) (ReadDouble(nPointPos + 16 * (iPoint)))
#define ReadY(iPoint) (ReadDouble(nPointPos + 16 * (iPoint) + 8))
#define ReadZ(iPoint) (ReadDouble(nPointPos + 16 * nNumPoints + 8 * (iPoint)))
#define ReadM(iPoint) (ReadDouble(nPointPos + 24 * nNumPoints + 8 * (iPoint)))

/************************************************************************/
/*                   sqlserver_geometry_parser()                           */
/************************************************************************/

sqlserver_geometry_parser::sqlserver_geometry_parser(spatial_data_type columnType)
{
    colType = columnType;
}

/************************************************************************/
/*                         ReadPoint()                                  */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadPoint(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();
    int iFigure = FigureOffset(iShape);
    if ( iFigure < nNumFigures ) {
        int iPoint = PointOffset(iFigure);
        if ( iPoint < nNumPoints ) {
            mapnik::geometry_type* point = new mapnik::geometry_type(mapnik::Point);
            if (colType == Geography) {
                point->move_to(ReadY(iPoint), ReadX(iPoint));
            } else {
                point->move_to(ReadX(iPoint), ReadY(iPoint));
            }
            geom->push_back(point);
        }
    }
    return geom;
}

/************************************************************************/
/*                         ReadMultiPoint()                             */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadMultiPoint(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();

    for (int i = iShape + 1; i < nNumShapes; i++) {
        if (ParentOffset(i) == (unsigned int)iShape) {
            if  ( ShapeType(i) == ST_POINT ) {
                mapnik::geometry_container *point = ReadPoint(i);
                for (size_t j=0; j<point->size(); j++) {
                    geom->push_back(&point->at(j));
                }
            }
        }
    }

    return geom;
}

/************************************************************************/
/*                         ReadLineString()                             */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadLineString(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();
    int iFigure = FigureOffset(iShape);

    mapnik::geometry_type *line = new mapnik::geometry_type(mapnik::LineString);
    int iPoint = PointOffset(iFigure);
    int iNextPoint = NextPointOffset(iFigure);
    if (colType == Geography) {
        line->move_to(ReadY(iPoint), ReadX(iPoint));
    } else {
        line->move_to(ReadX(iPoint), ReadY(iPoint));
    }
    ++iPoint;
    while (iPoint < iNextPoint) {
        if (colType == Geography) {
            line->line_to(ReadY(iPoint), ReadX(iPoint));
        } else {
            line->line_to(ReadX(iPoint), ReadY(iPoint));
        }
        
        ++iPoint;
    }
    geom->push_back(line);

    return geom;
}

/************************************************************************/
/*                         ReadMultiLineString()                        */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadMultiLineString(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();
    
    for (int i = iShape + 1; i < nNumShapes; i++) {
        if (ParentOffset(i) == (unsigned int)iShape) {
            if ( ShapeType(i) == ST_LINESTRING ) {
                mapnik::geometry_container *line = ReadLineString(i);
                for (size_t j=0; j<line->size(); j++) {
                    geom->push_back(&line->at(j));
                }
            }
        }
    }
    
    return geom;
}

/************************************************************************/
/*                         ReadPolygon()                                */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadPolygon(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();
    int iNextFigure = NextFigureOffset(iShape);
    
    for (int iFigure = FigureOffset(iShape); iFigure < iNextFigure; iFigure++) {
        mapnik::geometry_type *ring = new mapnik::geometry_type(mapnik::Polygon);
        int iPoint = PointOffset(iFigure);
        int iNextPoint = NextPointOffset(iFigure);
        if (colType == Geography) {
            ring->move_to(ReadY(iPoint), ReadX(iPoint));
        } else {
            ring->move_to(ReadX(iPoint), ReadY(iPoint));
        }
        ++iPoint;
        while (iPoint < iNextPoint) {
            if (colType == Geography) {
                ring->line_to(ReadY(iPoint), ReadX(iPoint));
            } else {
                ring->line_to(ReadX(iPoint), ReadY(iPoint));
            }

            ++iPoint;
        }
        geom->push_back(ring);
    }
    return geom;
}

/************************************************************************/
/*                         ReadMultiPolygon()                           */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadMultiPolygon(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();

    for (int i = iShape + 1; i < nNumShapes; i++) {
        if (ParentOffset(i) == (unsigned int)iShape) {
            if ( ShapeType(i) == ST_POLYGON ) {
                mapnik::geometry_container *poly = ReadPolygon(i);
                for (size_t j=0; j<poly->size(); j++) {
                    geom->push_back(&poly->at(j));
                }
            }
        }
    }

    return geom;
}

/************************************************************************/
/*                         ReadGeometryCollection()                     */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::ReadGeometryCollection(int iShape)
{
    mapnik::geometry_container *geom = new mapnik::geometry_container();

    for (int i = iShape + 1; i < nNumShapes; i++) {
        mapnik::geometry_container *shape = 0;
        if (ParentOffset(i) == (unsigned int)iShape) {
            switch (ShapeType(i))
            {
            case ST_POINT:
                shape = ReadPoint(i);
                break;
            case ST_LINESTRING:
                shape = ReadLineString(i);
                break;
            case ST_POLYGON:
                shape = ReadPolygon(i);
                break;
            case ST_MULTIPOINT:
                shape = ReadMultiPoint(i);
                break;
            case ST_MULTILINESTRING:
                shape = ReadMultiLineString(i);
                break;
            case ST_MULTIPOLYGON:
                shape = ReadMultiPolygon(i);
                break;
            case ST_GEOMETRYCOLLECTION:
                shape = ReadGeometryCollection(i);
                break;
            }
        }
        if (shape) {
            for (size_t j=0; j<shape->size(); j++) {
                geom->push_back(&shape->at(j));
            }
        }
    }

  return geom;
}


/************************************************************************/
/*                         parse_sql_geometry()                         */
/************************************************************************/

mapnik::geometry_container* sqlserver_geometry_parser::parse(unsigned char* pszInput, int nLen)
{
    if (nLen < 10) {
        throw sqlserver_geometry_parser_exception("not enough data");
    }
    
    pszData = pszInput;
    
    /* store the SRS id for further use */
    nSRSId = ReadInt32(0);
    
    if ( ReadByte(4) != 1 )
    {
        throw sqlserver_geometry_parser_exception("corrupt data");
    }

    chProps = ReadByte(5);

    if ( chProps & SP_HASMVALUES )
        nPointSize = 32;
    else if ( chProps & SP_HASZVALUES )
        nPointSize = 24;
    else
        nPointSize = 16;

    mapnik::geometry_container *geom = 0;
    if ( chProps & SP_ISSINGLEPOINT )
    {
        // single point geometry
        nNumPoints = 1;
        nPointPos = 6;

        if (nLen < 6 + nPointSize)
        {
            throw sqlserver_geometry_parser_exception("not enough data");
        }
        
        geom = new mapnik::geometry_container();
        mapnik::geometry_type *point = new mapnik::geometry_type(mapnik::Point);
        
        if (colType == Geography)
        {
            point->move_to(ReadY(0), ReadX(0));
        }
        else
        {
            point->move_to(ReadX(0), ReadY(0));
        }
        geom->push_back(point);
    }
    else if ( chProps & SP_ISSINGLELINESEGMENT )
    {
        // single line segment with 2 points
        nNumPoints = 2;
        nPointPos = 6;

        if (nLen < 6 + 2 * nPointSize)
        {
            throw sqlserver_geometry_parser_exception("not enough data");
        }

        geom = new mapnik::geometry_container();
        mapnik::geometry_type *line = new mapnik::geometry_type(mapnik::LineString);
        
        if (colType == Geography)
        {
            line->move_to(ReadY(0), ReadX(0));
            line->line_to(ReadY(1), ReadX(1));
        }
        else
        {
            line->move_to(ReadX(0), ReadY(0));
            line->line_to(ReadX(1), ReadY(1));
        }
        geom->push_back(line);
        
    }
    else
    {
        // complex geometries
        nNumPoints = ReadInt32(6);

        if ( nNumPoints <= 0 )
        {
            throw sqlserver_geometry_parser_exception("negative number of points");
        }

        // position of the point array
        nPointPos = 10;

        // position of the figures
        nFigurePos = nPointPos + nPointSize * nNumPoints + 4;
        
        if (nLen < nFigurePos)
        {
            throw sqlserver_geometry_parser_exception("not enough data");
        }

        nNumFigures = ReadInt32(nFigurePos - 4);

        if ( nNumFigures <= 0 )
        {
            throw sqlserver_geometry_parser_exception("negative number of figures");
        }
        
        // position of the shapes
        nShapePos = nFigurePos + 5 * nNumFigures + 4;

        if (nLen < nShapePos)
        {
            throw sqlserver_geometry_parser_exception("not enough data");
        }

        nNumShapes = ReadInt32(nShapePos - 4);

        if (nLen < nShapePos + 9 * nNumShapes)
        {
            throw sqlserver_geometry_parser_exception("not enough data");
        }

        if ( nNumShapes <= 0 )
        {
            throw sqlserver_geometry_parser_exception("negative number of shapes");
        }

        // pick up the root shape
        if ( ParentOffset(0) != 0xFFFFFFFF)
        {
            throw sqlserver_geometry_parser_exception("corrupt data");
        }

        // determine the shape type
        switch (ShapeType(0))
        {
        case ST_POINT:
            geom = ReadPoint(0);
            break;
        case ST_LINESTRING:
            geom = ReadLineString(0);
            break;
        case ST_POLYGON:
            geom = ReadPolygon(0);
            break;
        case ST_MULTIPOINT:
            geom = ReadMultiPoint(0);
            break;
        case ST_MULTILINESTRING:
            geom = ReadMultiLineString(0);
            break;
        case ST_MULTIPOLYGON:
            geom = ReadMultiPolygon(0);
            break;
        case ST_GEOMETRYCOLLECTION:
            geom = ReadGeometryCollection(0);
            break;
        default:
            throw sqlserver_geometry_parser_exception("unsupported geometry type");
        }
    }

    return geom;
}

