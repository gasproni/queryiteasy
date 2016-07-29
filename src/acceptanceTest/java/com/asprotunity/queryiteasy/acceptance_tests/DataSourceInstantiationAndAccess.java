package com.asprotunity.queryiteasy.acceptance_tests;

import javax.sql.DataSource;

public interface DataSourceInstantiationAndAccess {
    static DataSource instantiateDataSource(String fullyQualifiedClassName)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class<?> clazz = Class.forName(fullyQualifiedClassName);
        return (DataSource) clazz.newInstance();
    }

}
