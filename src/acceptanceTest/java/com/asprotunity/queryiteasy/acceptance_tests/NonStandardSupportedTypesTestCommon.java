package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.connection.Row;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class NonStandardSupportedTypesTestCommon extends SupportedTypesTestCommon {


    @Test
    public void stores_and_reads_longs_as_bigints() throws SQLException {
        Long value = 10L;
        List<Row> expectedValues = storeAndReadValuesBack("BIGINT", bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asLong(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asLong(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_booleans() throws SQLException {
        List<Row> expectedValues = storeAndReadValuesBack("BOOLEAN", bind((Boolean) null), bind(true));
        assertThat(expectedValues.size(), is(1));
        assertThat(asBoolean(expectedValues.get(0).at("first")), is(nullValue()));
        assertTrue(asBoolean(expectedValues.get(0).at("second")));
    }

    @Test
    public void stores_and_reads_times() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Row> expectedValues = storeAndReadValuesBack("TIME", bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asTime(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asTime(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_timestamps() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Row> expectedValues = storeAndReadValuesBack("TIMESTAMP", bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asTimestamp(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asTimestamp(expectedValues.get(0).at("second")), is(value));
    }


}
