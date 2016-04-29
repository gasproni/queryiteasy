package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.configureHSQLInMemoryDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HSQLSupportedTypesTest extends SupportedTypesTestCommon {


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

    @Test
    public void stores_and_reads_longs_as_bigints() throws SQLException {
        Long value = 10L;
        List<Row> expectedValues = storeAndReadValuesBack("BIGINT", bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asLong("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asLong("second"), is(value));
    }

    @Test
    public void stores_and_reads_doubles_mapped_to_double_correctly() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("DOUBLE", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDouble("second"), is(value));
    }

    @Test
    public void stores_and_reads_bytes_as_tinyints() throws SQLException {
        Byte value = 's';
        List<Row> expectedValues = storeAndReadValuesBack("TINYINT", bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asByte("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asByte("second"), is(value));
    }

    @Test
    public void stores_and_reads_booleans_correctly() throws SQLException {
        List<Row> expectedValues = storeAndReadValuesBack("BOOLEAN", bind((Boolean) null), bind(true));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asBoolean("first"), is(nullValue()));
        assertTrue(expectedValues.get(0).asBoolean("second"));
    }

    @Test
    public void stores_and_reads_times_correctly() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Row> expectedValues = storeAndReadValuesBack("TIME", bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asTime("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asTime("second"), is(value));
    }

    @Test
    public void stores_and_reads_timestamps_correctly() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Row> expectedValues = storeAndReadValuesBack("TIMESTAMP", bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asTimestamp("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asTimestamp("second"), is(value));
    }


}
