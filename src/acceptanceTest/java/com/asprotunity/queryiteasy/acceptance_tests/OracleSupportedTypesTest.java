package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Properties;

import static com.asprotunity.queryiteasy.acceptance_tests.DataSourceInstantiationAndAccess.instantiateDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;
import static org.junit.Assume.assumeTrue;

public class OracleSupportedTypesTest {

    private static DataStore dataStore;
    private static SupportedTypesTestDelegate tests;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Oracle JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
        tests = new SupportedTypesTestDelegate(dataStore);
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

        DataSource result = instantiateDataSource(properties.getProperty("queryiteasy.oracle.datasource.class"));

        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.url"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.password"));
        return result;

    }

    @After
    public void tearDown() throws Exception {
        dataStore.execute(
                connection -> connection.select(rs -> asString(rs, 1),
                                                "select 'drop '||object_type||' '|| object_name|| " +
                                                        "DECODE(OBJECT_TYPE,'TABLE',' CASCADE CONSTRAINTS','') from user_objects")
                        .filter(OracleSupportedTypesTest::isNotDropOfSystemOrLobIndex)
                        .forEach(statement -> connection.update(statement)));
    }

    @Test
    public void stores_and_reads_doubles_mapped_to_double() throws SQLException {
        tests.stores_and_reads_doubles_mapped_to_double("DOUBLE PRECISION");
    }

    @Test
    public void stores_and_reads_bytes_as_numbers() throws SQLException {
        tests.stores_and_reads_bytes_as("NUMBER");
    }

    @Test
    public void stores_and_reads_blobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_blobs_as("BLOB");
    }

    @Test
    public void stores_and_reads_clobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_clobs("CLOB");
    }

    @Test
    public void stores_and_reads_bytes_arrays() throws SQLException {
        tests.stores_and_reads_bytes_arrays("RAW(1000)");
    }

    @Test
    public void stores_and_reads_times() throws SQLException {
        tests.stores_and_reads_times("DATE");
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
}
