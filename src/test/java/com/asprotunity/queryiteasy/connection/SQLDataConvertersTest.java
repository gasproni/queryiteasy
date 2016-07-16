package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.stringio.StringIO;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Scanner;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static com.asprotunity.queryiteasy.stringio.StringIO.readFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class SQLDataConvertersTest {

    @Test
    public void converts_null_byte_arrays() {
        assertThat(asByteArray(null), is(nullValue()));
    }

    @Test
    public void converts_byte_arrays() {
        byte[] value = new byte[] {12, 13, 14};
        assertThat(asByteArray(value), is(value));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_byte_array_not_possible() {
        Integer value = 1;
        assert_throws_class_cast_exception(value, SQLDataConverters::asByteArray, byte[].class);
    }

    @Test
    public void converts_null_strings() {
        assertThat(asString(null), is(nullValue()));
    }

    @Test
    public void converts_strings() {
        assertThat(asString("text"), is("text"));
    }

    @Test
    public void converts_to_boolean_from_nullL() {
        assertThat(asBoolean(null), is(nullValue()));
    }

    @Test
    public void converts_to_boolean() {
        assertThat(asBoolean(Boolean.TRUE), is(Boolean.TRUE));
        assertThat(asBoolean(Boolean.FALSE), is(Boolean.FALSE));
    }

    @Test
    public void converts_string_to_boolean() {
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
    public void converts_to_big_decimal_from_nullL() {
        assertThat(asBigDecimal(null), is(nullValue()));
    }

    @Test
    public void converts_to_big_decimal() {
        assertThat(asBigDecimal(BigDecimal.TEN), is(BigDecimal.TEN));
    }

    @Test
    public void converts_big_decimal_from_string() {
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
    public void converts_to_sql_date_from_nullL() {
        assertThat(asDate(null), is(nullValue()));
    }

    @Test
    public void converts_sql_date() {
        Date date = new Date(123456);
        assertThat(asDate(date), is(date));
    }

    @Test
    public void converts_sql_date_from_string() {
        String value = "2016-05-31";
        assertThat(asDate(value), is(Date.valueOf(value)));
    }

    @Test
    public void converts_sql_date_from_sql_timestamp() {
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
    public void converts_to_sql_time_from_nullL() {
        assertThat(asTime(null), is(nullValue()));
    }

    @Test
    public void converts_sql_time() {
        Time time = new Time(123456);
        assertThat(asTime(time), is(time));
    }

    @Test
    public void converts_sql_time_from_string() {
        String value = "23:12:33";
        assertThat(asTime(value), is(Time.valueOf(value)));
    }

    @Test
    public void converts_sql_time_from_sql_timestamp() {
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
    public void converts_to_sql_timestamp_from_nullL() {
        assertThat(asTimestamp(null), is(nullValue()));
    }

    @Test
    public void converts_sql_timestamp() {
        Timestamp timestamp = new Timestamp(123456);
        assertThat(asTimestamp(timestamp), is(timestamp));
    }

    @Test
    public void converts_sql_timestamp_from_string() {
        String value = "2016-02-27 21:12:30.333";
        assertThat(asTimestamp(value), is(Timestamp.valueOf(value)));
    }

    @Test
    public void throws_class_cast_exception_when_conversion_to_sql_timestamp_not_possible() {
        Character value = 20;
        assert_throws_class_cast_exception(value, SQLDataConverters::asTimestamp, Timestamp.class);
    }

    @Test
    public void converts_to_number_from_nullL() {
        assertThat(convertNumber(null, Integer.class, Integer::valueOf, Number::intValue), is(nullValue()));
    }

    @Test
    public void converts_to_number() {
        Integer value = 10;
        assertThat(convertNumber(value, Integer.class, Integer::valueOf, Number::intValue), is(value));
    }

    @Test
    public void converts_to_number_from_different_numeric_type() {
        Double toConvert = 10.0;
        Integer expected = toConvert.intValue();
        assertThat(convertNumber(toConvert, Integer.class, Integer::valueOf, Number::intValue), is(expected));
    }

    @Test
    public void converts_to_number_from_string() {
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
    public void from_blob_converts_from_sql_blob() throws IOException, SQLException {
        Blob blob = mock(Blob.class);
        String expected = "this is a string to be converted;";
        Charset charset = Charset.forName("UTF-8");
        when(blob.getBinaryStream()).thenReturn(new ByteArrayInputStream(expected.getBytes("UTF-8")));

        String converted = fromBlob(blob, inputStream -> readFrom(inputStream, charset));

        assertThat(converted, is(expected));
    }

    @Test
    public void from_blob_frees_resources() throws IOException, SQLException {
        Blob blob = mock(Blob.class);
        ByteArrayInputStream blobInputStream = mock(ByteArrayInputStream.class);
        when(blob.getBinaryStream()).thenReturn(blobInputStream);

        fromBlob(blob, inputStream -> "doesn't matter");

        InOrder order = inOrder(blob, blobInputStream);
        order.verify(blob, times(1)).getBinaryStream();
        order.verify(blobInputStream, times(1)).close();
        order.verify(blob, times(1)).free();
    }

    @Test
    public void returns_null_if_blob_is_null() throws UnsupportedEncodingException, SQLException {
        String converted = fromBlob(null, inputStream -> {
            if (inputStream == null)
                return null;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(nullValue()));
    }

    @Test
    public void from_blob_converts_from_byte_array() throws UnsupportedEncodingException {
        String expected = "this is a string to be converted;";
        Charset charset = Charset.forName("UTF-8");
        byte[] bytes = expected.getBytes("UTF-8");

        String converted = fromBlob(bytes, inputStream -> readFrom(inputStream, charset));

        assertThat(converted, is(expected));
    }

    @Test
    public void from_clob_converts_from_sql_clob() throws UnsupportedEncodingException, SQLException {
        Clob clob = mock(Clob.class);
        String expected = "this is a string to be converted;";
        when(clob.getCharacterStream()).thenReturn(new StringReader(expected));

        String converted = fromClob(clob, StringIO::readFrom);

        assertThat(converted, is(expected));
    }

    @Test
    public void from_clob_frees_resources() throws IOException, SQLException {
        Clob clob = mock(Clob.class);
        Reader reader = mock(Reader.class);
        when(clob.getCharacterStream()).thenReturn(reader);

        fromClob(clob, stream -> "doesn't matter");

        InOrder order = inOrder(clob, reader);
        order.verify(clob, times(1)).getCharacterStream();
        order.verify(reader, times(1)).close();
        order.verify(clob, times(1)).free();
    }

    @Test
    public void returns_null_if_clob_is_null() throws UnsupportedEncodingException, SQLException {
        String converted = fromClob(null, reader -> {
            if (reader == null)
                return null;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(nullValue()));
    }

    @Test
    public void from_clob_converts_from_string() throws UnsupportedEncodingException {
        String expected = "this is a string to be converted;";

        String converted = fromClob(expected, reader -> new Scanner(reader).useDelimiter("\\A").next());

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