package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.stringio.StringIO;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
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