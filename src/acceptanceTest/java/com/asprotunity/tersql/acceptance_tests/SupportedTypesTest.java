package com.asprotunity.tersql.acceptance_tests;


import com.asprotunity.tersql.connection.Row;
import com.asprotunity.tersql.connection.StatementParameter;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static com.asprotunity.tersql.connection.StatementParameter.bind;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SupportedTypesTest extends EndToEndTestBase {


    @Test
    public void handles_integers_correctly() throws SQLException {
        Integer value = 10;
        List<Row> expectedValues = storeAndReadValuesBack("INTEGER", bind((Integer) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asInteger("second"), is(value));
    }

    @Test
    public void handles_short_integers_correctly() throws SQLException {
        Short value = 10;
        List<Row> expectedValues = storeAndReadValuesBack("SMALLINT", bind((Short) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asShort("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asShort("second"), is(value));
    }

    @Test
    public void handles_longs_correctly() throws SQLException {
        Long value = 10L;
        List<Row> expectedValues = storeAndReadValuesBack("BIGINT", bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asLong("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asLong("second"), is(value));
    }

    @Test
    public void handles_doubles_mapped_to_double_correctly() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("DOUBLE", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDouble("second"), is(value));
    }

    @Test
    public void handles_doubles_mapped_to_float_correctly() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("FLOAT", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDouble("second"), is(value));
    }

    @Test
    public void handles_floats_correctly() throws SQLException {
        Float value = 10.0F;
        List<Row> expectedValues = storeAndReadValuesBack("REAL", bind((Float) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asFloat("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asFloat("second"), is(value));
    }

    @Test
    public void handles_bytes_correctly() throws SQLException {
        Byte value = 's';
        List<Row> expectedValues = storeAndReadValuesBack("TINYINT", bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asByte("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asByte("second"), is(value));
    }

    @Test
    public void handles_big_decimals_as_decimal_correctly() throws SQLException {
        BigDecimal value = new BigDecimal(10);
        List<Row> expectedValues = storeAndReadValuesBack("DECIMAL", bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asBigDecimal("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asBigDecimal("second"), is(value));
    }

    @Test
    public void handles_big_decimals_as_numeric_correctly() throws SQLException {
        BigDecimal value = new BigDecimal(10);
        List<Row> expectedValues = storeAndReadValuesBack("NUMERIC", bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asBigDecimal("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asBigDecimal("second"), is(value));
    }

    @Test
    public void handles_booleans_correctly() throws SQLException {
        List<Row> expectedValues = storeAndReadValuesBack("BOOLEAN", bind((Boolean) null), bind(true));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asBoolean("first"), is(nullValue()));
        assertTrue(expectedValues.get(0).asBoolean("second"));
    }

    @Test
    public void handles_dates_correctly() throws SQLException {
        // Fri, 01 Jan 2016 00:00:00 GMT It's important that the time is all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        java.sql.Date value = new Date(1451606400000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Date) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDate("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDate("second"), is(value));
    }

    @Test
    public void handles_times_correctly() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        java.sql.Time value = new Time(36672000L);
        List<Row> expectedValues = storeAndReadValuesBack("TIME", bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asTime("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asTime("second"), is(value));
    }

    @Test
    public void handles_timestamps_correctly() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.345 GMT
        java.sql.Timestamp value = new Timestamp(1452593472345L);
        List<Row> expectedValues = storeAndReadValuesBack("TIMESTAMP", bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asTimestamp("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asTimestamp("second"), is(value));
    }


    private List<Row> storeAndReadValuesBack(String sqlType, StatementParameter firstValue, StatementParameter secondValue) {
        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first " + sqlType + " NULL, second " + sqlType + " NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    firstValue, secondValue);
        });

        return dataStore.executeWithResult(connection ->
                connection.select("SELECT * FROM testtable", rowStream -> rowStream.collect(toList()))
        );
    }
}
