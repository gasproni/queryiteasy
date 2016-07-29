package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.configureHSQLInMemoryDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;

public class HSQLSupportedTypesTest {

    private static DataStore dataStore;
    private static SupportedTypesTestDelegate tests;

    @BeforeClass
    public static void setUp() throws Exception {
        dataStore = new DataStore(configureHSQLInMemoryDataSource());
        tests = new SupportedTypesTestDelegate(dataStore);
    }

    @After
    public void tearDown() throws Exception {
        dropHSQLPublicSchema(dataStore);
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
    public void stores_and_reads_blobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_blobs_as("BLOB");
    }

    @Test
    public void stores_and_reads_clobs() throws SQLException, UnsupportedEncodingException {
        tests.stores_and_reads_clobs("CLOB");
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
