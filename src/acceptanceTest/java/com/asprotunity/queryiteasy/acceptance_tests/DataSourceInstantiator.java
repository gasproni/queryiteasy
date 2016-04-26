package com.asprotunity.queryiteasy.acceptance_tests;

import org.hsqldb.jdbc.JDBCDataSource;

import javax.sql.DataSource;

public class DataSourceInstantiator {
    public static DataSource instantiateDataSource(String fullyQualifiedClassName)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class<?> clazz = Class.forName(fullyQualifiedClassName);
        return (DataSource) clazz.newInstance();
    }

    static DataSource configureHSQLInMemoryDataSource() throws Exception {
        JDBCDataSource result = new JDBCDataSource();
        result.setUrl("jdbc:hsqldb:mem:testdb");
        result.setUser("sa");
        result.setPassword("");
        return result;

    }
}
