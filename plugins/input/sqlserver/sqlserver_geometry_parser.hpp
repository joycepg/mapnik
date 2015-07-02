// mapnik
#include <mapnik/geometry.hpp>

enum spatial_data_type {
    Geometry,
    Geography
};

class sqlserver_geometry_parser
{
protected:    
    unsigned char* pszData;
    /* serialization propeties */
    char chProps;
    /* point array */
    int nPointSize;
    int nPointPos;
    int nNumPoints;
    /* figure array */
    int nFigurePos;
    int nNumFigures;
    /* shape array */
    int nShapePos;
    int nNumShapes;
    int nSRSId;
    /* geometry or geography */
    spatial_data_type colType;

protected:
    mapnik::geometry_container* ReadPoint(int iShape);
    mapnik::geometry_container* ReadMultiPoint(int iShape);
    mapnik::geometry_container* ReadLineString(int iShape);
    mapnik::geometry_container* ReadMultiLineString(int iShape);
    mapnik::geometry_container* ReadPolygon(int iShape);
    mapnik::geometry_container* ReadMultiPolygon(int iShape);
    mapnik::geometry_container* ReadGeometryCollection(int iShape);

public:
    sqlserver_geometry_parser(spatial_data_type columnType);
    
    mapnik::geometry_container* parse(unsigned char* pszInput, int nLen);
    int get_srs_id() { return nSRSId; };
};


