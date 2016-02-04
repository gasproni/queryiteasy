package com.asprotunity.tersql.internal;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Function;

import static com.asprotunity.tersql.internal.TypeConverters.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TypeConvertersTest {

    @Test
    public void converts_to_boolean_from_nullL_correctly() {
        assertThat(toBoolean(null), is(nullValue()));
    }

    @Test
    public void converts_to_boolean_correctly() {
        assertThat(toBoolean(Boolean.TRUE), is(Boolean.TRUE));
        assertThat(toBoolean(Boolean.FALSE), is(Boolean.FALSE));
    }

    @Test
    public void converts_string_to_boolean_correctly() {
        assertThat(toBoolean("true"), is(Boolean.TRUE));
        assertThat(toBoolean("True"), is(Boolean.TRUE));
        assertThat(toBoolean("TRUE"), is(Boolean.TRUE));
        assertThat(toBoolean("false"), is(Boolean.FALSE));
        assertThat(toBoolean("False"), is(Boolean.FALSE));
        assertThat(toBoolean("FALSE"), is(Boolean.FALSE));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_boolean_not_possible() {
        Integer value = 1;
        assert_throws_class_cast_exception(value, TypeConverters::toBoolean, Boolean.class);
    }

    @Test
    public void converts_to_big_decimal_from_nullL_correctly() {
        assertThat(toBigDecimal(null), is(nullValue()));
    }

    @Test
    public void converts_to_big_decimal_correctly() {
        assertThat(toBigDecimal(BigDecimal.TEN), is(BigDecimal.TEN));
    }

    @Test
    public void converts_big_decimal_from_string_correctly() {
        assertThat(toBigDecimal(new BigDecimal("10")), is(BigDecimal.TEN));
        assertThat(toBigDecimal(new BigDecimal("10.0")), is(BigDecimal.valueOf(10.0d)));
    }

    @Test
    public void converts_big_decimal_from_number_to_its_double_equivalent() {
        Integer value = 10;
        assertThat(toBigDecimal(value), is(new BigDecimal("10.0")));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_big_decimal_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, TypeConverters::toBigDecimal, BigDecimal.class);
    }

    @Test
    public void converts_to_sql_date_from_nullL_correctly() {
        assertThat(toSqlDate(null), is(nullValue()));
    }

    @Test
    public void converts_sql_date_correctly() {
        Date date = new Date(123456);
        assertThat(toSqlDate(date), is(date));
    }

    @Test
    public void converts_sql_date_from_string_correctly() {
        String value = "2016-05-31";
        assertThat(toSqlDate(value), is(Date.valueOf(value)));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_date_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, TypeConverters::toSqlDate, Date.class);
    }

    @Test
    public void converts_to_sql_time_from_nullL_correctly() {
        assertThat(toSqlTime(null), is(nullValue()));
    }

    @Test
    public void converts_sql_time_correctly() {
        Time time = new Time(123456);
        assertThat(toSqlTime(time), is(time));
    }

    @Test
    public void converts_sql_time_from_string_correctly() {
        String value = "23:12:33";
        assertThat(toSqlTime(value), is(Time.valueOf(value)));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_time_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, TypeConverters::toSqlTime, Time.class);
    }

    @Test
    public void converts_to_sql_timestamp_from_nullL_correctly() {
        assertThat(toSqlTimestamp(null), is(nullValue()));
    }

    @Test
    public void converts_sql_timestamp_correctly() {
        Timestamp timestamp = new Timestamp(123456);
        assertThat(toSqlTimestamp(timestamp), is(timestamp));
    }

    @Test
    public void converts_sql_timestamp_from_string_correctly() {
        String value = "2016-02-27 21:12:30.333";
        assertThat(toSqlTimestamp(value), is(Timestamp.valueOf(value)));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_timestamp_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, TypeConverters::toSqlTimestamp, Timestamp.class);
    }

    private <T> void assert_throws_class_cast_exception(Object toConvert, Function<Object, T> convertType, Class<T> targetType) {
        try {
            convertType.apply(toConvert);
            fail("Exception expected!");
        } catch (java.lang.ClassCastException exc) {
            assertThat(exc.getMessage(), is(toConvert.getClass().getCanonicalName() + " cannot be cast to " +
                    targetType.getCanonicalName()));
        }
    }
}