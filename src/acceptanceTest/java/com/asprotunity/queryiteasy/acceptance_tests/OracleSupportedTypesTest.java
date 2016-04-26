package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.configureHSQLInMemoryDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;
import static com.asprotunity.queryiteasy.acceptance_tests.OracleConfigurationAndSchemaDrop.dropSchemaObjects;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OracleSupportedTypesTest extends SupportedTypesTestBase {


    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = OracleConfigurationAndSchemaDrop.configureDataSource();
        dataStore = new DataStore(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        OracleConfigurationAndSchemaDrop.dropSchemaObjects(getDataStore());
    }

    protected DataStore getDataStore() {
        return dataStore;
    }

    @Test
    public void handles_longs_as_numbers() throws SQLException {
        Long value = 10L;
        List<Row> expectedValues = storeAndReadValuesBack("NUMBER", bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asLong("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asLong("second"), is(value));
    }

    @Test
    public void handles_doubles_as_double_precision() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("DOUBLE PRECISION", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDouble("second"), is(value));
    }

    @Test
    public void handles_bytes_as_smallints() throws SQLException {
        Byte value = 's';
        List<Row> expectedValues = storeAndReadValuesBack("SMALLINT", bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asByte("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asByte("second"), is(value));
    }

    @Test
    public void handles_times_as_sql_dates() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asTime("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asTime("second"), is(value));
    }

    @Test
    public void handles_sql_timestamps_as_dates() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDate("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDate("second"), is(value));
    }

    @Override
    protected void cleanup() throws Exception {
        dropSchemaObjects(getDataStore());
    }

}
