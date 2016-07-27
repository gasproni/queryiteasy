package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.ResultSetReaders;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
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

    @Test
    public void stores_and_reads_doubles_mapped_to_double() throws SQLException {
        Double value = 10.0;
        List<Tuple2<Double, Double>> expectedValues = storeAndReadValuesBack("DOUBLE",
                ResultSetReaders::asDouble, bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_bytes_as_tinyints() throws SQLException {
        Byte value = 's';
        List<Tuple2<Byte, Byte>> expectedValues = storeAndReadValuesBack("TINYINT",
                ResultSetReaders::asByte, bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select(
                    rs -> asString(rs, "dropTableStatement"),
                    "SELECT CONCAT('DROP TABLE ', table_name, ' CASCADE') as dropTableStatement" +
                            " FROM information_schema.tables WHERE table_schema = ?",
                    bind(dbName)).collect(toList());
            for (String statement : dropTableStatements) {
                connection.update(statement);
            }
        });
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
    }

}
