/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2015 we-do-IT
 *
 *****************************************************************************/

#ifndef SQLSERVER_FEATURESET_HPP
#define SQLSERVER_FEATURESET_HPP

// mapnik
#include <mapnik/feature.hpp>
#include <mapnik/datasource.hpp>
#include <mapnik/geometry.hpp>
#include <mapnik/unicode.hpp>

// boost
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

// sql server (via odbc)
#include "sql.h"

// spatial data types
#include "sqlserver_datasource.hpp"

class sqlserver_featureset : public mapnik::Featureset
{
public:
    sqlserver_featureset(SQLHDBC hdbc,
                         mapnik::context_ptr const& ctx,
                         std::string const& sqlstring,
                         std::string const& encoding,
                         spatial_data_type columntype);
    virtual ~sqlserver_featureset();
    mapnik::feature_ptr next();

private:
    SQLHANDLE hstmt_;
    boost::scoped_ptr<mapnik::transcoder> tr_;
    spatial_data_type column_type_;
    mapnik::value_integer feature_id_;
    mapnik::context_ptr ctx_;
};

#endif // SQLSERVER_FEATURESET_HPP
