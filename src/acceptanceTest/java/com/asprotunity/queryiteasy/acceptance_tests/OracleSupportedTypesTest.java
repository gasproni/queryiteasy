package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static com.asprotunity.queryiteasy.acceptance_tests.OracleConfigurationAndSchemaDrop.dropSchemaObjects;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class OracleSupportedTypesTest extends SupportedTypesTestCommon {

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = OracleConfigurationAndSchemaDrop.configureDataSource();
        assumeTrue("No Oracle JDBC driver found, skipping tests", dataSource != null);
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
    public void can_handle_longs_as_numbers() throws SQLException {
        Long value = 10L;
        List<Row> expectedValues = storeAndReadValuesBack("NUMBER", bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asLong("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asLong("second"), is(value));
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


    @Test
    public void doesnt_support_booleans() throws SQLException {
        try {
            storeAndReadValuesBack("BOOLEAN", bind((Boolean) null), bind(true));
        } catch (RuntimeSQLException exc) {
            assertThat(exc.getCause().getMessage(), is("ORA-00902: invalid datatype\n"));
        }
    }

    @Override
    protected void cleanup() throws Exception {
        dropSchemaObjects(getDataStore());
    }

}
