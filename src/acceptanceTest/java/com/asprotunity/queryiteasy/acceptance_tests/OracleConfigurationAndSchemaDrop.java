package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public interface OracleConfigurationAndSchemaDrop {
    static void dropSchemaObjects(DataStore dataStore) {
        dataStore.execute(connection -> {
            List<String> dropStatements = connection.select("select 'drop '||object_type||' '|| object_name|| " +
                            "DECODE(OBJECT_TYPE,'TABLE',' CASCADE CONSTRAINTS','') as dropStatements from user_objects",
                    rowStream -> rowStream.map(row -> row.asString("dropStatements")).collect(Collectors.toList()));
            for (String statement : dropStatements) {
                if (isNotDropOfSystemOrLobIndex(statement)) {
                    connection.update(statement);
                }
            }
        });
    }

    static boolean isNotDropOfSystemOrLobIndex(String statement) {
        return !statement.startsWith("drop INDEX SYS_") && !statement.startsWith("drop LOB");
    }

    static DataSource configureDataSource() throws Exception {

        Path path = Paths.get("test_datasources", "oracle.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = PropertiesLoader.loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.oracle.datasource.class"));

        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.url"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.password"));
        return result;

    }
}
