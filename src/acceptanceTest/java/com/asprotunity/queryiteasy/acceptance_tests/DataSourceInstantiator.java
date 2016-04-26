package com.asprotunity.queryiteasy.acceptance_tests;

import org.hsqldb.jdbc.JDBCDataSource;

import javax.sql.DataSource;

public class DataSourceInstantiator {
    public static DataSource instantiateDataSource(String fullyQualifiedClassName)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class<?> clazz = Class.forName(fullyQualifiedClassName);
        return (DataSource) clazz.newInstance();
    }

}
