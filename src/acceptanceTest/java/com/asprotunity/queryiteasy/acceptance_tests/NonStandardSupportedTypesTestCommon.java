package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.connection.Row;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class NonStandardSupportedTypesTestCommon extends SupportedTypesTestCommon {


    @Test
    public void stores_and_reads_longs_as_bigints() throws SQLException {
        Long value = 10L;
        List<Tuple2<Long, Long>> expectedValues = storeAndReadValuesBack("BIGINT", Row::asLong, bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_booleans() throws SQLException {
        List<Tuple2<Boolean, Boolean>> expectedValues = storeAndReadValuesBack("BOOLEAN", Row::asBoolean, bind((Boolean) null), bind(true));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertTrue(expectedValues.get(0)._2);
    }

    @Test
    public void stores_and_reads_times() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Tuple2<Time, Time>> expectedValues = storeAndReadValuesBack("TIME", Row::asTime, bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_timestamps() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Tuple2<Timestamp, Timestamp>> expectedValues = storeAndReadValuesBack("TIMESTAMP", Row::asTimestamp, bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }


}
