package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DefaultDataStore;
import org.hsqldb.jdbc.JDBCDataSource;

import javax.sql.DataSource;

public interface HSQLInMemoryConfigurationAndSchemaDrop {
    static DataSource configureHSQLInMemoryDataSource() throws Exception {
        JDBCDataSource result = new JDBCDataSource();
        result.setUrl("jdbc:hsqldb:mem:testdb");
        result.setUser("sa");
        result.setPassword("");
        return result;

    }

    static void dropHSQLPublicSchema(DefaultDataStore dataStore) {
        dataStore.execute(connection -> connection.update("DROP SCHEMA PUBLIC CASCADE"));
    }
}
