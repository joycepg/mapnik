/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2015 we-do-IT
 *
 *****************************************************************************/

#include "sqlserver_featureset.hpp"
#include "sqlserver_datasource.hpp"
#include "sqlserver_geometry_parser.hpp"

// mapnik
#include <mapnik/global.hpp>
#include <mapnik/debug.hpp>
#include <mapnik/box2d.hpp>
#include <mapnik/geometry.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/feature_layer_desc.hpp>
#include <mapnik/wkb.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/feature_factory.hpp>

// sql server (odbc)
#include <sqlext.h>
#include <msodbcsql.h>

using mapnik::query;
using mapnik::box2d;
using mapnik::feature_ptr;
using mapnik::geometry_type;
using mapnik::geometry_utils;
using mapnik::transcoder;
using mapnik::feature_factory;

sqlserver_featureset::sqlserver_featureset(SQLHDBC hdbc,
                                 mapnik::context_ptr const& ctx,
                                 std::string const& sqlstring,
                                 std::string const& encoding,
                                 spatial_data_type columntype
                                 )
    : hstmt_(0),
      tr_(new transcoder(encoding)),
      column_type_(columntype),
      feature_id_(1),
      ctx_(ctx)
{
    SQLRETURN retcode;
    
    // allocate statement handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt_);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not allocate statement", SQL_HANDLE_DBC, hdbc);
    }
    
    // execute statement
    retcode = SQLExecDirect(hstmt_, (SQLCHAR*)sqlstring.c_str(), SQL_NTS);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not execute statement", SQL_HANDLE_STMT, hstmt_);
    }
    

}

sqlserver_featureset::~sqlserver_featureset() {
    if (hstmt_) {
        (void)SQLFreeStmt(hstmt_, SQL_CLOSE);
        hstmt_ = 0;
    }
}

feature_ptr sqlserver_featureset::next()
{
    SQLRETURN retcode;
    
    // fetch next result
    retcode = SQLFetch(hstmt_);
    if (retcode == SQL_NO_DATA) {
        // normal end of recordset
        return feature_ptr();
    }
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not fetch result", SQL_HANDLE_STMT, hstmt_);
    }
    
    // create an empty feature with the next id
    feature_ptr feature(feature_factory::create(ctx_, feature_id_));

    
    // find out how many columns in result set
    SQLSMALLINT n=0;
    retcode = SQLNumResultCols(hstmt_, &n);
    if (!SQL_SUCCEEDED(retcode)) {
        throw sqlserver_datasource_exception("could not get number of result columns", SQL_HANDLE_STMT, hstmt_);
    }
    
    // get name,type for each column
    for (int ColumnNum=1; ColumnNum<=n; ColumnNum++) {
        SQLCHAR      ColumnName[255]; // max is currently 128 in sql server
        SQLSMALLINT  NameLength;
        SQLSMALLINT  DataType;
        SQLULEN      ColumnSize;
        SQLSMALLINT  DecimalDigits;
        SQLSMALLINT  Nullable;
        retcode = SQLDescribeCol(hstmt_, ColumnNum, ColumnName, sizeof(ColumnName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
        if (!SQL_SUCCEEDED(retcode)) {
            throw sqlserver_datasource_exception("could not describe column", SQL_HANDLE_STMT, hstmt_);
        }
        
        SQLCHAR sval[2048];
        long ival;
        double dval;
        SQLCHAR BinaryPtr[2048];    // TODO: handle larger
        SQLLEN BinaryLenOrInd;
        SQLLEN LenOrInd;
        switch (DataType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case -9: // NVARCHAR
            case SQL_DATETIME:
            case SQL_TYPE_DATE:
            case SQL_TYPE_TIME:
            case SQL_TYPE_TIMESTAMP:
                retcode = SQLGetData(hstmt_, ColumnNum, SQL_C_CHAR, sval, sizeof(sval), &LenOrInd);
                if (!SQL_SUCCEEDED(retcode)) {
                    throw sqlserver_datasource_exception("could not get data", SQL_HANDLE_STMT, hstmt_);
                }
                feature->put((char*)ColumnName, (UnicodeString)tr_->transcode((char*)sval));
                break;
                
            case SQL_INTEGER:
            case SQL_SMALLINT:
                retcode = SQLGetData(hstmt_, ColumnNum, SQL_C_SLONG, &ival, sizeof(ival), &LenOrInd);
                if (!SQL_SUCCEEDED(retcode)) {
                    throw sqlserver_datasource_exception("could not get data", SQL_HANDLE_STMT, hstmt_);
                }
                feature->put((char*)ColumnName, static_cast<mapnik::value_integer>(ival));
                break;
                
            case SQL_NUMERIC:
            case SQL_DECIMAL:
            case SQL_FLOAT:
            case SQL_REAL:
            case SQL_DOUBLE:
                retcode = SQLGetData(hstmt_, ColumnNum, SQL_C_DOUBLE, &dval, sizeof(dval), &LenOrInd);
                if (!SQL_SUCCEEDED(retcode)) {
                    throw sqlserver_datasource_exception("could not get data", SQL_HANDLE_STMT, hstmt_);
                }
                feature->put((char*)ColumnName, dval);
                break;
    
            case SQL_SS_UDT: {
                // make sure this udt is a spatial data type
                retcode = SQLGetData(hstmt_, ColumnNum, SQL_C_BINARY, BinaryPtr, sizeof(BinaryPtr), &BinaryLenOrInd);
                if (!SQL_SUCCEEDED(retcode)) {
                    throw sqlserver_datasource_exception("could not get data size", SQL_HANDLE_STMT, hstmt_);
                }
    
                sqlserver_geometry_parser geometry_parser(column_type_);
                mapnik::geometry_container *geom = geometry_parser.parse(BinaryPtr, BinaryLenOrInd);
                for (size_t j=0; j<geom->size(); j++) {
                    feature->add_geometry(&geom->at(j));
                }
                break;
            }

            default:
                MAPNIK_LOG_WARN(sqlserver) << "sqlserver_datasource: unknown/unsupported datatype in column: " << ColumnName << " (" << DataType << ")";
                break;
        }
    }
    ++feature_id_;
    
    return feature;
}

