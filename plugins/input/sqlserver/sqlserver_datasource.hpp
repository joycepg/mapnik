/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2015 we-do-IT
 *
 *****************************************************************************/

#ifndef SQLSERVER_DATASOURCE_HPP
#define SQLSERVER_DATASOURCE_HPP

// mapnik
#include <mapnik/datasource.hpp>
#include <mapnik/params.hpp>
#include <mapnik/query.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/box2d.hpp>
#include <mapnik/coord.hpp>
#include <mapnik/feature_layer_desc.hpp>
#include <mapnik/value_types.hpp>

// boost
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>

// stl
#include <vector>
#include <string>

// sql server (via odbc)
#include "sql.h"

enum spatial_data_type {
    Geometry,
    Geography
};


class sqlserver_datasource : public mapnik::datasource
{
public:
    sqlserver_datasource(mapnik::parameters const& params);
    virtual ~sqlserver_datasource ();
    static const char * name();
    mapnik::datasource::datasource_t type() const;
    mapnik::featureset_ptr features(mapnik::query const& q) const;
    mapnik::featureset_ptr features_at_point(mapnik::coord2d const& pt, double tol = 0) const;
    mapnik::box2d<double> envelope() const;
    boost::optional<mapnik::datasource::geometry_t> get_geometry_type() const;
    mapnik::layer_descriptor get_descriptor() const;

private:
    mapnik::datasource::datasource_t type_;
    
    std::string table_;
    std::string fields_;
    std::string geometry_field_;
    spatial_data_type geometry_type_;

    mutable bool extent_initialized_;
    mutable mapnik::box2d<double> extent_;
    mutable int srid_;

    mapnik::layer_descriptor desc_;

    SQLHENV henv_;
    SQLHDBC hdbc_;
    
    mapnik::featureset_ptr features_in_box(mapnik::box2d<double> const& box) const;
};

class sqlserver_datasource_exception : public mapnik::datasource_exception
{
public:
    sqlserver_datasource_exception(std::string const& message);
    sqlserver_datasource_exception(std::string const& message, SQLSMALLINT HandleType, SQLHANDLE Handle);
    
    virtual ~sqlserver_datasource_exception() throw();
    static std::string sql_diagnostics(SQLSMALLINT HandleType, SQLHANDLE Handle);
};

#endif // SQLSERVER_DATASOURCE_HPP
