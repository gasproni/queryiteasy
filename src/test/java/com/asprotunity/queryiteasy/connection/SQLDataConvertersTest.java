package com.asprotunity.queryiteasy.connection;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Scanner;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SQLDataConvertersTest {

    @Test
    public void converts_to_boolean_from_nullL_correctly() {
        assertThat(asBoolean(null), is(nullValue()));
    }

    @Test
    public void converts_to_boolean_correctly() {
        assertThat(asBoolean(Boolean.TRUE), is(Boolean.TRUE));
        assertThat(asBoolean(Boolean.FALSE), is(Boolean.FALSE));
    }

    @Test
    public void converts_string_to_boolean_correctly() {
        assertThat(asBoolean("true"), is(Boolean.TRUE));
        assertThat(asBoolean("True"), is(Boolean.TRUE));
        assertThat(asBoolean("TRUE"), is(Boolean.TRUE));
        assertThat(asBoolean("false"), is(Boolean.FALSE));
        assertThat(asBoolean("False"), is(Boolean.FALSE));
        assertThat(asBoolean("FALSE"), is(Boolean.FALSE));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_boolean_not_possible() {
        Integer value = 1;
        assert_throws_class_cast_exception(value, SQLDataConverters::asBoolean, Boolean.class);
    }

    @Test
    public void converts_to_big_decimal_from_nullL_correctly() {
        assertThat(asBigDecimal(null), is(nullValue()));
    }

    @Test
    public void converts_to_big_decimal_correctly() {
        assertThat(asBigDecimal(BigDecimal.TEN), is(BigDecimal.TEN));
    }

    @Test
    public void converts_big_decimal_from_string_correctly() {
        assertThat(asBigDecimal(new BigDecimal("10")), is(BigDecimal.TEN));
        assertThat(asBigDecimal(new BigDecimal("10.0")), is(BigDecimal.valueOf(10.0d)));
    }

    @Test
    public void converts_big_decimal_from_number_to_its_double_equivalent() {
        Integer value = 10;
        assertThat(asBigDecimal(value), is(new BigDecimal("10.0")));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_big_decimal_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, SQLDataConverters::asBigDecimal, BigDecimal.class);
    }

    @Test
    public void converts_to_sql_date_from_nullL_correctly() {
        assertThat(asDate(null), is(nullValue()));
    }

    @Test
    public void converts_sql_date_correctly() {
        Date date = new Date(123456);
        assertThat(asDate(date), is(date));
    }

    @Test
    public void converts_sql_date_from_string_correctly() {
        String value = "2016-05-31";
        assertThat(asDate(value), is(Date.valueOf(value)));
    }

    @Test
    public void converts_sql_date_from_sql_timestamp_correctly() {
        Timestamp timestamp = Timestamp.valueOf("2016-02-27 21:12:30.333");
        Date expectedDate = Date.valueOf("2016-02-27");
        assertThat(asDate(timestamp).toLocalDate(), is(expectedDate.toLocalDate()));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_date_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, SQLDataConverters::asDate, Date.class);
    }

    @Test
    public void converts_to_sql_time_from_nullL_correctly() {
        assertThat(asTime(null), is(nullValue()));
    }

    @Test
    public void converts_sql_time_correctly() {
        Time time = new Time(123456);
        assertThat(asTime(time), is(time));
    }

    @Test
    public void converts_sql_time_from_string_correctly() {
        String value = "23:12:33";
        assertThat(asTime(value), is(Time.valueOf(value)));
    }

    @Test
    public void converts_sql_time_from_sql_timestamp_correctly() {
        Timestamp timestamp = Timestamp.valueOf("2016-02-27 21:12:30.333");
        Time expectedTime = Time.valueOf("21:12:30");
        assertThat(asTime(timestamp).toLocalTime(), is(expectedTime.toLocalTime()));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_time_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, SQLDataConverters::asTime, Time.class);
    }

    @Test
    public void converts_to_sql_timestamp_from_nullL_correctly() {
        assertThat(asTimestamp(null), is(nullValue()));
    }

    @Test
    public void converts_sql_timestamp_correctly() {
        Timestamp timestamp = new Timestamp(123456);
        assertThat(asTimestamp(timestamp), is(timestamp));
    }

    @Test
    public void converts_sql_timestamp_from_string_correctly() {
        String value = "2016-02-27 21:12:30.333";
        assertThat(asTimestamp(value), is(Timestamp.valueOf(value)));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_timestamp_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, SQLDataConverters::asTimestamp, Timestamp.class);
    }

    @Test
    public void converts_to_number_from_nullL_correctly() {
        assertThat(convertNumber(null, Integer.class, Integer::valueOf, Number::intValue), is(nullValue()));
    }

    @Test
    public void converts_to_number_correctly() {
        Integer value = 10;
        assertThat(convertNumber(value, Integer.class, Integer::valueOf, Number::intValue), is(value));
    }

    @Test
    public void converts_to_number_from_different_numeric_type_correctly() {
        Double toConvert = 10.0;
        Integer expected = toConvert.intValue();
        assertThat(convertNumber(toConvert, Integer.class, Integer::valueOf, Number::intValue), is(expected));
    }

    @Test
    public void converts_to_number_from_string_correctly() {
        String toConvert = "10";
        Integer expected = Integer.parseInt(toConvert);
        assertThat(convertNumber(toConvert, Integer.class, Integer::valueOf, Number::intValue), is(expected));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_number_not_possible() {
        try {
            Character toConvert = 10;
            convertNumber(toConvert, Integer.class, Integer::valueOf, Number::intValue);
            fail("Exception expected!");
        } catch (java.lang.ClassCastException exc) {
            assertThat(exc.getMessage(), is(Character.class.getCanonicalName() + " cannot be cast to " +
                    Integer.class.getCanonicalName()));
        }
    }

    @Test
    public void converts_from_blob_byte_array() throws UnsupportedEncodingException {
        String expected = "this is a string to be converted;";
        String charset = "UTF-8";
        byte[] bytes = expected.getBytes(charset);

        String converted = fromBlob(bytes, optInputStream -> new Scanner(optInputStream.get(), charset).useDelimiter("\\A").next());

        assertThat(converted, is(expected));
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