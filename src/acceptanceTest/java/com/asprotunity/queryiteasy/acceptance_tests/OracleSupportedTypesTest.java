package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DefaultDataStore;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static java.util.stream.Collectors.toList;
import static org.junit.Assume.assumeTrue;

public class OracleSupportedTypesTest extends SupportedTypesTestCommon {

    private static DefaultDataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Oracle JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DefaultDataStore(dataSource);
    }

    private static boolean isNotDropOfSystemOrLobIndex(String statement) {
        String statementToLower = statement.toLowerCase();
        return !statementToLower.startsWith("drop index sys_") && !statementToLower.startsWith("drop lob");
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("oracle.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = TestPropertiesLoader.loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.oracle.datasource.class"));

        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.url"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.password"));
        return result;

    }

    protected DefaultDataStore getDataStore() {
        return dataStore;
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropStatements = connection.select(row -> row.asString("dropStatements"),
                    "select 'drop '||object_type||' '|| object_name|| " +
                            "DECODE(OBJECT_TYPE,'TABLE',' CASCADE CONSTRAINTS','') as dropStatements from user_objects"
            ).collect(toList());
            for (String statement : dropStatements) {
                if (isNotDropOfSystemOrLobIndex(statement)) {
                    connection.update(statement);
                }
            }
        });
    }

}
