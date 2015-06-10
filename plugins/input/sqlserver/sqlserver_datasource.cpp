/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2015 we-do-IT
 *
 *****************************************************************************/

#include "sqlserver_datasource.hpp"
#include "sqlserver_featureset.hpp"
#include "sqlserver_geometry_parser.hpp"

// mapnik
#include <mapnik/debug.hpp>
#include <mapnik/boolean.hpp>
#include <mapnik/sql_utils.hpp>
#include <mapnik/timer.hpp>
#include <mapnik/value_types.hpp>

// boost
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/make_shared.hpp>

// stl
#include <string>
#include <algorithm>
#include <set>
#include <sstream>
#include <iomanip>

// sql server (odbc)
#include <sqlext.h>
#include <msodbcsql.h>

using mapnik::datasource;
using mapnik::parameters;
using mapnik::query;
using mapnik::featureset_ptr;
using mapnik::layer_descriptor;
using mapnik::attribute_descriptor;
using mapnik::datasource_exception;
using mapnik::box2d;
using mapnik::coord2d;

DATASOURCE_PLUGIN(sqlserver_datasource)

// an exception class to wrap gathering the odbc error
sqlserver_datasource_exception::sqlserver_datasource_exception(std::string const& message)
    : mapnik::datasource_exception("SQL Server Plugin: "+message) {
}

sqlserver_datasource_exception::sqlserver_datasource_exception(std::string const& message, SQLSMALLINT HandleType, SQLHANDLE Handle)
    : mapnik::datasource_exception("SQL Server Plugin: "+message+": "+sql_diagnostics(HandleType, Handle)) {
}

sqlserver_datasource_exception::~sqlserver_datasource_exception() throw () {
}

std::string sqlserver_datasource_exception::sql_diagnostics(SQLSMALLINT HandleType, SQLHANDLE Handle) {
    // Get the status records.
    std::ostringstream s;
    SQLCHAR SqlState[6];
    SQLINTEGER NativeError;
    SQLCHAR Msg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT MsgLen;
    SQLSMALLINT i = 1;
    while (SQLGetDiagRec(HandleType, Handle, i, SqlState, &NativeError, Msg, sizeof(Msg), &MsgLen) == SQL_SUCCESS) {
        s << "[" << SqlState << "] ";
        s << Msg;
        s << " (" << NativeError << ") ";
        i++;
    }
    return s.str();
}

// datasource
sqlserver_datasource::sqlserver_datasource(parameters const& params) :
    datasource (params),
    type_(datasource::Vector),
    fields_(*params.get<std::string>("fields", "*")),
    geometry_field_(*params.get<std::string>("geometry_field", "")),
    geometry_type_(Geometry),
    extent_initialized_(false),
    desc_(*params.get<std::string>("type"), *params.get<std::string>("encoding", "utf-8")),
    henv_(0),
    hdbc_(0)
{
#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "sqlserver_datasource::init");
#endif
    
    // they must supply a table/view name or a subquery
    if (params.get<std::string>("table")) {
        table_ = *params.get<std::string>("table");
    } else {
        throw sqlserver_datasource_exception("no <table> parameter specified");
    }
    
    // the driver refers to an entry in odbcinst.ini http://www.unixodbc.org/odbcinst.html
    std::string driver("ODBC Driver 11 for SQL Server"); // default for ms driver
    if (params.get<std::string>("driver")) {
        driver = *params.get<std::string>("driver");
    }
 
    // build the connection string
    std::ostringstream s;
    s << "Driver={" << driver << "};";
    if (params.get<std::string>("server")) {
        s << "Server=" << *params.get<std::string>("server") << ";";
    }
    if (params.get<std::string>("database")) {
        s << "Database=" << *params.get<std::string>("database") << ";";
    }
    if (params.get<std::string>("user")) {
        s << "Uid=" << *params.get<std::string>("user") << ";";
    }
    if (params.get<std::string>("password")) {
        s << "Pwd=" << *params.get<std::string>("password") << ";";
    }
    if (params.get<std::string>("trusted")) {
        s << "Trusted_Connection=" << *params.get<std::string>("trusted") << ";";
    }
    std::string InConnectionString = s.str();
    MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: connection string: " << InConnectionString;
    
    // everything returns one of these
    SQLRETURN retcode;

    // allocate environment handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv_);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not allocate environment handle"); // no diagnostics available
    }

    // set the ODBC version environment attribute
    retcode = SQLSetEnvAttr(henv_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not set ODBC version environment value", SQL_HANDLE_ENV, henv_);
    }
    
    // allocate connection handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv_, &hdbc_);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not allocate connection handle", SQL_HANDLE_ENV, henv_);
    }
    
    // set login timeout to 5 seconds
    retcode = SQLSetConnectAttr(hdbc_, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not set connection timeout", SQL_HANDLE_DBC, hdbc_);
    }

    // connect to data source
    SQLCHAR OutConnectionString[1024];
    SQLSMALLINT OutConnectionStringLength;
    retcode = SQLDriverConnect(hdbc_,
                               NULL,
                               (SQLCHAR*)InConnectionString.c_str(),
                               InConnectionString.length(),
                               OutConnectionString,
                               1024,
                               &OutConnectionStringLength,
                               SQL_DRIVER_NOPROMPT);

    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not connect", SQL_HANDLE_DBC, hdbc_);
    }
    MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: connected: " << OutConnectionString;
    
    // get columns description
#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "sqlserver_datasource::get_column_description");
#endif

    // table parameter can be a table/view name or a subquery
    // iff a subquery, need to wrap in ()
    std::ostringstream stmt;
    stmt << "SELECT TOP(1) " << fields_ << " FROM ";
    if (table_.find_first_of(" \t") == std::string::npos) {
        // no whitespace in table; assume a table/view name
        stmt << table_;
    } else {
        // whitespace in table; assume a subquery
        stmt << "(" << table_ << ") T";
    }
    MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: " << stmt.str();
    
    // allocate statement handle
    SQLHANDLE hstmt=0;
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not allocate statement", SQL_HANDLE_DBC, hdbc_);
    }

    // prepare statement
    retcode = SQLPrepare(hstmt, (SQLCHAR*)stmt.str().c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not prepare statement", SQL_HANDLE_STMT, hstmt);
    }
    
    // find out how many columns in result set
    SQLSMALLINT n=0;
    retcode = SQLNumResultCols(hstmt, &n);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not get number of result columns", SQL_HANDLE_STMT, hstmt);
    }
    
    // get name,type for each column
    for (int i=1; i<=n; i++) {
        SQLCHAR      ColumnName[255]; // max is currently 128 in sql server
        SQLSMALLINT  NameLength;
        SQLSMALLINT  DataType;
        SQLULEN      ColumnSize;
        SQLSMALLINT  DecimalDigits;
        SQLSMALLINT  Nullable;
        retcode = SQLDescribeCol(hstmt, i, ColumnName, sizeof(ColumnName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
        if (!SQL_SUCCEEDED(retcode)) {
            throw sqlserver_datasource_exception("could not describe column", SQL_HANDLE_STMT, hstmt);
        }
        
        SQLCHAR TypeName[255];
        SQLSMALLINT ReturnedLength;
        char geometry[] = {'g','\0','e','\0','o','\0','m','\0','e','\0','t','\0','r','\0','y','\0','\0','\0'}; // geometry
        char geography[] = {'g','\0','e','\0','o','\0','g','\0','r','\0','a','\0','p','\0','h','\0','y','\0','\0','\0'}; // geography
        switch (DataType) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case -9: // NVARCHAR
            desc_.add_descriptor(attribute_descriptor((char *)ColumnName,mapnik::String));
            //MAPNIK_LOG_DEBUG(sqlserver) << "found string column: " << (char*)ColumnName;
            break;
                
        case SQL_INTEGER:
        case SQL_SMALLINT:
            desc_.add_descriptor(attribute_descriptor((char *)ColumnName,mapnik::Integer));
            //MAPNIK_LOG_DEBUG(sqlserver) << "found integer column: " << (char*)ColumnName;
            break;
                
        case SQL_NUMERIC:
        case SQL_DECIMAL:
        case SQL_FLOAT:
        case SQL_REAL:
        case SQL_DOUBLE:
            desc_.add_descriptor(attribute_descriptor((char *)ColumnName,mapnik::Double));
            //MAPNIK_LOG_DEBUG(sqlserver) << "found double column: " << (char*)ColumnName;
            break;
                
        case SQL_DATETIME:
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP:
            desc_.add_descriptor(attribute_descriptor((char *)ColumnName,mapnik::String));
            //MAPNIK_LOG_DEBUG(sqlserver) << "found string column: " << (char*)ColumnName;
            break;
                
        case SQL_SS_UDT:
            // check if it is a geometry type
            retcode = SQLColAttribute(hstmt, i, SQL_CA_SS_UDT_TYPE_NAME, &TypeName, sizeof(TypeName), &ReturnedLength, NULL);
            if (!SQL_SUCCEEDED(retcode)) {
                throw sqlserver_datasource_exception("could not get column attribute", SQL_HANDLE_STMT, hstmt);
            }
            // on linux the type name is returned as a weird string with null bytes as every second character
            if (strcmp((char *)TypeName, "geometry") == 0 || std::memcmp((char*)TypeName, geometry, ReturnedLength) == 0) {
                //MAPNIK_LOG_DEBUG(sqlserver) << "found geometry column: " << (char*)ColumnName;
                geometry_field_ = (const char *)ColumnName;
                geometry_type_ = Geometry;
            }
            if (strcmp((char *)TypeName, "geography") == 0 || std::memcmp((char*)TypeName, geography, ReturnedLength) == 0) {
                //MAPNIK_LOG_DEBUG(sqlserver) << "found geography column: " << (char*)ColumnName;
                geometry_field_ = (const char *)ColumnName;
                geometry_type_ = Geography;
            }
            break;

        default:
            MAPNIK_LOG_WARN(sqlserver) << "sqlserver_datasource: unknown/unsupported datatype in column: " << ColumnName << " (" << DataType << ")";
            break;
        }
    }

    // free handle
    retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not free statement handle", SQL_HANDLE_STMT, hstmt);
    }

    // final check
    if (geometry_field_ == "") {
         MAPNIK_LOG_WARN(sqlserver) << "sqlserver_datasource: no geometry column found or specified";
    }

}

sqlserver_datasource::~sqlserver_datasource()
{
    if (hdbc_) {
        (void)SQLDisconnect(hdbc_);
        (void)SQLFreeHandle(SQL_HANDLE_DBC, hdbc_);
        hdbc_ = 0;
    }
    if (henv_) {
        (void)SQLFreeHandle(SQL_HANDLE_ENV, henv_);
        henv_ = 0;
    }
}

const char * sqlserver_datasource::name()
{
    return "sqlserver";
}

mapnik::datasource::datasource_t sqlserver_datasource::type() const
{
    return type_;
}

box2d<double> sqlserver_datasource::envelope() const
{
    if (extent_initialized_) return extent_;

    SQLRETURN retcode;
    
    // allocate statement handle
    SQLHANDLE hstmt=0;
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not allocate statement", SQL_HANDLE_DBC, hdbc_);
    }
    
    // table parameter can be a table/view or a subquery
    // iff a subquery, need to wrap in ()
    std::ostringstream stmt;
    stmt << "SELECT geometry::EnvelopeAggregate(" << geometry_field_ << ") FROM ";
    if (table_.find_first_of(" \t") == std::string::npos) {
        // no whitespace in table; assume a table/view name
        stmt << table_;
    } else {
        // whitespace in table; assume a subquery
        stmt << "(" << table_ << ") T";
    }
    MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: " << stmt.str();
    
    // execute statement
    retcode = SQLExecDirect(hstmt, (SQLCHAR*)stmt.str().c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not execute statement", SQL_HANDLE_STMT, hstmt);
    }

    // fetch first result (will only be one row)
    retcode = SQLFetch(hstmt);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not fetch result", SQL_HANDLE_STMT, hstmt);
    }
    
    // get the row data
    SQLUSMALLINT ColumnNum = 1;
    SQLCHAR BinaryPtr[1024];    // envelope is a 5 point polygon; usually only 112 bytes
    SQLLEN BinaryLenOrInd;
    retcode = SQLGetData(hstmt, ColumnNum, SQL_C_BINARY, BinaryPtr, sizeof(BinaryPtr), &BinaryLenOrInd);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not get data", SQL_HANDLE_STMT, hstmt);
    }
    //MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: envelope returned " << BinaryLenOrInd << " bytes";
    
    sqlserver_geometry_parser geometry_parser(geometry_type_);
    mapnik::geometry_container *geom = geometry_parser.parse(BinaryPtr, BinaryLenOrInd);
    if (geom->size() > 0) {
        extent_ = geom->at(0).envelope();
        extent_initialized_ = true;
        srid_ = geometry_parser.get_srs_id(); // get the srid of the extents; assume that is same for whole table
    }
    
    // free handle
    retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not free statement handle", SQL_HANDLE_STMT, hstmt);
    }
    
    return extent_;
}

boost::optional<mapnik::datasource::geometry_t> sqlserver_datasource::get_geometry_type() const
{
    return boost::optional<mapnik::datasource::geometry_t>();
}

layer_descriptor sqlserver_datasource::get_descriptor() const
{
    return desc_;
}

std::string sqlserver_datasource::sql_bbox(box2d<double> const& env) const
{
    std::ostringstream b;
    b << std::setprecision(16);
    b << "geometry::STPolyFromText('POLYGON((";
    b << env.minx() << " " << env.miny() << ", ";
    b << env.minx() << " " << env.maxy() << ", ";
    b << env.maxx() << " " << env.maxy() << ", ";
    b << env.maxx() << " " << env.miny() << ", ";
    b << env.minx() << " " << env.miny() << "))'," << srid_ <<")";
    return b.str();
}

featureset_ptr sqlserver_datasource::features(query const& q) const
{
#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "sqlserver_datasource::features");
#endif
    box2d<double> const& box = q.get_bbox();
    
    std::ostringstream s;
    s << "SELECT ";
    s << geometry_field_;

    std::set<std::string> const& props = q.property_names();
    std::set<std::string>::const_iterator pos = props.begin();
    std::set<std::string>::const_iterator end = props.end();
    mapnik::context_ptr ctx = boost::make_shared<mapnik::context_type>();
    for (; pos != end; ++pos) {
        s << ", " << *pos;
        ctx->push(*pos);
    }
    
    std::string clause = table_; //populate_tokens(table_, scale_denom, box, px_gw, px_gh);
    
    std::ostringstream spatial_sql;
    spatial_sql << " WHERE ";
    spatial_sql << geometry_field_ << ".STIntersects(" << sql_bbox(box) << ") = 1";
    
    if (boost::algorithm::ifind_first(clause, "WHERE"))
    {
        boost::algorithm::ireplace_first(clause, "WHERE", spatial_sql.str() + " AND ");
    }
    else if (boost::algorithm::ifind_first(clause, table_))
    {
        boost::algorithm::ireplace_first(clause, table_, table_ + " " + spatial_sql.str());
    }
    else
    {
        MAPNIK_LOG_WARN(sqlserver) << "sqlserver_datasource: cannot determine where to add the spatial filter clause";
    }
    
    s << " FROM " << clause;

    MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: " << s.str();

    return boost::make_shared<sqlserver_featureset>(hdbc_,
                                                ctx,
                                                s.str(),
                                                desc_.get_encoding(),
                                                geometry_type_);
}

featureset_ptr sqlserver_datasource::features_at_point(coord2d const& pt, double tol) const
{
#ifdef MAPNIK_STATS
    mapnik::progress_timer __stats__(std::clog, "sqlserver_datasource::features_at_point");
#endif
    box2d<double> box(pt.x - tol, pt.y - tol, pt.x + tol, pt.y + tol);
    
    std::ostringstream s;
    s << "SELECT ";
    s << geometry_field_;
    
    std::vector<attribute_descriptor>::const_iterator itr = desc_.get_descriptors().begin();
    std::vector<attribute_descriptor>::const_iterator end = desc_.get_descriptors().end();
    mapnik::context_ptr ctx = boost::make_shared<mapnik::context_type>();
    while (itr != end)
    {
        s << ", " << itr->get_name();
        ctx->push(itr->get_name());
        ++itr;
    }
    
    std::string clause = table_; //populate_tokens(table_, scale_denom, box, px_gw, px_gh);
    
    std::ostringstream spatial_sql;
    spatial_sql << " WHERE ";
    spatial_sql << geometry_field_ << ".STIntersects(" << sql_bbox(box) << ") = 1";
    
    if (boost::algorithm::ifind_first(clause, "WHERE"))
    {
        boost::algorithm::ireplace_first(clause, "WHERE", spatial_sql.str() + " AND ");
    }
    else if (boost::algorithm::ifind_first(clause, table_))
    {
        boost::algorithm::ireplace_first(clause, table_, table_ + " " + spatial_sql.str());
    }
    else
    {
        MAPNIK_LOG_WARN(sqlserver) << "sqlserver_datasource: cannot determine where to add the spatial filter clause";
    }
    
    s << " FROM " << clause;
    
    MAPNIK_LOG_DEBUG(sqlserver) << "sqlserver_datasource: " << s.str();
    
    return boost::make_shared<sqlserver_featureset>(hdbc_,
                                                    ctx,
                                                    s.str(),
                                                    desc_.get_encoding(),
                                                    geometry_type_);
}

