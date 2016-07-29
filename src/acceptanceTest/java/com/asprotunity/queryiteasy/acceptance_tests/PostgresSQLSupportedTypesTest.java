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
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;
import static org.junit.Assume.assumeTrue;

public class PostgresSQLSupportedTypesTest {

    private static DataStore dataStore;
    private static SupportedTypesTestDelegate tests;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Postgresql JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
        tests = new SupportedTypesTestDelegate(dataStore);
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("postgresql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = instantiateDataSource(properties.getProperty("queryiteasy.postgresql.datasource.class"));

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

    @After
    public void tearDown() throws Exception {
        dataStore.execute(
                connection -> connection.select(rs -> asString(rs, "dropTableStatement"),
                                                "select 'drop table if exists \"' || tablename || '\" cascade;' as dropTableStatement" +
                                                        "  from pg_tables " +
                                                        " where tableowner = 'testuser'")
                        .forEach(statement -> connection.update(statement)));
    }

    @Test
    public void stores_and_reads_doubles_mapped_to_double() throws SQLException {
        tests.stores_and_reads_doubles_mapped_to_double("DOUBLE PRECISION");
    }

    @Test
    public void stores_and_reads_bytes_as_integers() throws SQLException {
        tests.stores_and_reads_bytes_as("INTEGER");
    }

    @Test
    public void stores_and_reads_blobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_blobs_as("OID");
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
}
