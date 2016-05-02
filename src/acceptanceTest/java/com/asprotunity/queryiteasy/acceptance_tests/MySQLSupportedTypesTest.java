package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asString;
import static org.junit.Assume.assumeTrue;

public class MySQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DataStore dataStore;
    private static String dbName;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No MySQL JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select("SELECT CONCAT('DROP TABLE ', table_name, ' CASCADE') as dropTableStatement" +
                            " FROM information_schema.tables WHERE table_schema = ?",
                    rowStream -> rowStream.map(row -> asString(row.at("dropTableStatement"))).collect(Collectors.toList()),
                    bind(dbName));
            for (String statement : dropTableStatements) {
                connection.update(statement);
            }
        });
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("mysql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.mysql.datasource.class"));

        String dbURL = properties.getProperty("queryiteasy.mysql.datasource.url");
        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, dbURL);

        dbName = dbURL.substring(dbURL.lastIndexOf("/") + 1);

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.mysql.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.mysql.datasource.password"));
        return result;

    }

}
