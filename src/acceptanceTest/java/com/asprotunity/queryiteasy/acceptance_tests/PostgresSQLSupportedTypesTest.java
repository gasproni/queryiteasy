package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DefaultDataStore;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static java.util.stream.Collectors.toList;
import static org.junit.Assume.assumeTrue;

public class PostgresSQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DefaultDataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Postgresql JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DefaultDataStore(dataSource);
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("postgresql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.postgresql.datasource.class"));

        String dbName = properties.getProperty("queryiteasy.postgresql.databaseName");
        Method setDbName = result.getClass().getMethod("setDatabaseName", String.class);
        setDbName.invoke(result, dbName);

        Method setServerName = result.getClass().getMethod("setServerName", String.class);
        setServerName.invoke(result, properties.getProperty("queryiteasy.postgresql.serverName"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.postgresql.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.postgresql.datasource.password"));
        return result;

    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select(row -> row.asString("dropTableStatement"),
                    "select 'drop table if exists \"' || tablename || '\" cascade;' as dropTableStatement" +
                            "  from pg_tables " +
                            " where tableowner = 'testuser'"
            ).collect(toList());
            for (String statement : dropTableStatements) {
                connection.update(statement);
            }
        });
    }

    @Override
    protected DefaultDataStore getDataStore() {
        return dataStore;
    }

}
