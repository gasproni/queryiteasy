package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;
import org.hsqldb.jdbc.JDBCDataSource;

import javax.sql.DataSource;

public abstract class HSQLInMemoryHelpers {
    public static DataSource configureHSQLInMemoryDataSource() throws Exception {
        JDBCDataSource result = new JDBCDataSource();
        result.setUrl("jdbc:hsqldb:mem:testdb");
        result.setUser("sa");
        result.setPassword("");
        return result;

    }

    public static void dropHSQLInMemorySchema(DataStore dataStore) {
        dataStore.execute(connection -> connection.update("DROP SCHEMA PUBLIC CASCADE"));
    }
}
