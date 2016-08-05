package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.connection.StringOutputParameter;
import com.asprotunity.queryiteasy.datastore.DataStore;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Properties;

import static com.asprotunity.queryiteasy.acceptance_tests.DataSourceInstantiationAndAccess.instantiateDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bindString;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class MySQLSupportedTypesTest {

    private static DataStore dataStore;
    private static SupportedTypesTestDelegate tests;
    private static String dbName;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No MySQL JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
        tests = new SupportedTypesTestDelegate(dataStore);
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("mysql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = instantiateDataSource(properties.getProperty("queryiteasy.mysql.datasource.class"));

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

    @After
    public void tearDown() throws Exception {
        dataStore.execute(connection -> {
            connection.select(rs -> asString(rs, 1),
                              "SELECT CONCAT('DROP TABLE ', table_name, ' CASCADE')" +
                                      " FROM information_schema.tables WHERE table_schema = ?",
                              bindString(dbName))
                    .forEach(statement -> connection.update(statement));

            connection.select(rs -> asString(rs, 1),
                              "SELECT CONCAT('DROP ',routine_type,' `',routine_schema,'`.`',routine_name,'`;')" +
                                      " FROM information_schema.routines WHERE routine_schema = ?",
                              bindString(dbName))
                    .forEach(statement -> connection.update(statement));


        });

    }

    @Test
    public void stores_and_reads_doubles_mapped_to_double() throws SQLException {
        tests.stores_and_reads_doubles_mapped_to_double("DOUBLE");
    }

    @Test
    public void stores_and_reads_bytes_as_tinyints() throws SQLException {
        tests.stores_and_reads_bytes_as("TINYINT");
    }

    @Test
    public void stores_and_reads_bytes_as_integers() throws SQLException {
        tests.stores_and_reads_bytes_as("INTEGER");
    }

    @Test
    public void stores_and_reads_bytes_arrays() throws SQLException {
        tests.stores_and_reads_bytes_arrays("LONG VARBINARY");
    }

    @Test
    public void stores_and_reads_blobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_blobs_as("BLOB");
    }

    @Test
    public void stores_and_reads_clobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_clobs("TEXT");
    }

    @Test
    public void stores_and_reads_longs_as_bigints() throws SQLException {
        tests.stores_and_reads_longs_as_bigints();
    }

    @Test
    public void stores_and_reads_booleans() throws SQLException {
        tests.stores_and_reads_booleans();
    }

    @Test
    public void stores_and_reads_times() throws SQLException {
        tests.stores_and_reads_times("TIME");
    }

    @Test
    public void stores_and_reads_timestamps() throws SQLException {
        tests.stores_and_reads_timestamps();
    }

    @Test
    public void stores_and_reads_integers() throws SQLException {
        tests.stores_and_reads_integers();
    }

    @Test
    public void stores_and_reads_strings() throws SQLException {
        tests.stores_and_reads_strings();
    }

    @Test
    public void stores_and_reads_short_integers() throws SQLException {
        tests.stores_and_reads_short_integers();
    }

    @Test
    public void stores_and_reads_doubles_as_floats() throws SQLException {
        tests.stores_and_reads_doubles_as_floats();
    }

    @Test
    public void stores_and_reads_floats() throws SQLException {
        tests.stores_and_reads_floats();
    }

    @Test
    public void stores_and_reads_big_decimals_as_decimal() throws SQLException {
        tests.stores_and_reads_big_decimals_as_decimal();
    }

    @Test
    public void stores_and_reads_big_decimals_as_numeric() throws SQLException {
        tests.stores_and_reads_big_decimals_as_numeric();
    }

    @Test
    public void stores_and_reads_dates() throws SQLException {
        tests.stores_and_reads_dates();
    }

    @Test
    public void stores_and_reads_bytes_as_smallints() throws SQLException {
        tests.stores_and_reads_bytes_as_smallints();
    }

    @Test
    public void calls_function_with_result_in_output_param() throws ParseException {

        String expected = "result";
        dataStore.execute(
                connection -> connection.update("CREATE FUNCTION `return_string`()\n" +
                                                        "RETURNS VARCHAR(10)\n" +
                                                        "DETERMINISTIC\n" +
                                                        "BEGIN\n" +
                                                        "   RETURN ?;\n" +
                                                        "END", bindString(expected))
        );

        StringOutputParameter outputParameter = new StringOutputParameter();
        dataStore.execute(
                connection -> connection.call("{? = call return_string() }", outputParameter)
        );

        assertThat(outputParameter.value(), is(expected));
    }
}
