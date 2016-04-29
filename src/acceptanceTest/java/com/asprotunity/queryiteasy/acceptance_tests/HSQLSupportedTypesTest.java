package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import org.junit.BeforeClass;

import javax.sql.DataSource;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.configureHSQLInMemoryDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;

public class HSQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureHSQLInMemoryDataSource();
        dataStore = new DataStore(dataSource);
    }

    @Override
    protected void cleanup() throws Exception {
        dropHSQLPublicSchema(getDataStore());
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
    }

}
