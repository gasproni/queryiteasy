package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.connection.ResultSetReaders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class ResultSetReadersTest {

    private final int columnIndex = 5;
    private final String columnLabel = "columnLabel";
    private final ResultSet resultSet = mock(ResultSet.class);
    private InputStream inputStream = mock(InputStream.class);
    private Reader reader = mock(Reader.class);

    @Test
    public void columnCount_reads_columncount_from_resultset_metadata() throws SQLException, IOException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        int columnCount = 13;
        when(metadata.getColumnCount()).thenReturn(columnCount);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(metadata);

        assertThat(columnCount(resultSet), is(columnCount));
        verify(resultSet, times(1)).getMetaData();
        verify(metadata, times(1)).getColumnCount();
    }

    @Test
    public void fromBinaryStream_reads_from_binary_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnIndex)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = fromBinaryStream(resultSet, columnIndex, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getBinaryStream(columnIndex);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void fromBinaryStream_reads_from_binary_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnLabel)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = fromBinaryStream(resultSet, columnLabel, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getBinaryStream(columnLabel);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void fromBinaryStream_reads_from_null_binary_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnIndex)).thenReturn(null);

        Integer result = fromBinaryStream(resultSet, columnIndex, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getBinaryStream(columnIndex);
    }

    @Test
    public void reads_from_null_binary_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnLabel)).thenReturn(null);

        Integer result = fromBinaryStream(resultSet, columnLabel, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getBinaryStream(columnLabel);
    }

    @Test
    public void fromBinaryStream_throws_InvalidArgumentException_when_stream_reader_parameter_null() {
        assertThrows(() -> fromBinaryStream(resultSet, columnIndex, null), InvalidArgumentException.class);
        assertThrows(() -> fromBinaryStream(resultSet, columnLabel, null), InvalidArgumentException.class);
    }

    @Test
    public void fromAsciiStream_reads_from_ascii_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnIndex)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = fromAsciiStream(resultSet, columnIndex, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getAsciiStream(columnIndex);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void fromAsciiStream_reads_from_ascii_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnLabel)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = fromAsciiStream(resultSet, columnLabel, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getAsciiStream(columnLabel);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void fromAsciiStream_reads_from_null_ascii_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnIndex)).thenReturn(null);

        Integer result = fromAsciiStream(resultSet, columnIndex, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getAsciiStream(columnIndex);
    }

    @Test
    public void fromAsciiStream_reads_from_null_ascii_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnLabel)).thenReturn(null);

        Integer result = fromAsciiStream(resultSet, columnLabel, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getAsciiStream(columnLabel);
    }

    @Test
    public void fromAsciiStream_throws_InvalidArgumentException_when_stream_reader_parameter_null() {
        assertThrows(() -> fromAsciiStream(resultSet, columnIndex, null), InvalidArgumentException.class);
        assertThrows(() -> fromAsciiStream(resultSet, columnLabel, null), InvalidArgumentException.class);
    }

    @Test
    public void fromCharacterStream_reads_from_character_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnIndex)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = fromCharacterStream(resultSet, columnIndex, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getCharacterStream(columnIndex);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void fromCharacterStream_reads_from_character_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnLabel)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = fromCharacterStream(resultSet, columnLabel, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getCharacterStream(columnLabel);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void fromCharacterStream_reads_from_null_character_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnIndex)).thenReturn(null);

        Integer result = fromCharacterStream(resultSet, columnIndex, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getCharacterStream(columnIndex);
    }

    @Test
    public void fromCharacterStream_reads_from_null_character_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnLabel)).thenReturn(null);

        Integer result = fromCharacterStream(resultSet, columnLabel, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getCharacterStream(columnLabel);
    }

    @Test
    public void fromCharacterStream_throws_InvalidArgumentException_when_stream_reader_parameter_null() {
        assertThrows(() -> fromCharacterStream(resultSet, columnIndex, null), InvalidArgumentException.class);
        assertThrows(() -> fromCharacterStream(resultSet, columnLabel, null), InvalidArgumentException.class);
    }

    @Test
    public void fromNCharacterStream_reads_from_ncharacter_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnIndex)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = fromNCharacterStream(resultSet, columnIndex, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getNCharacterStream(columnIndex);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void fromNCharacterStream_reads_from_ncharacter_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnLabel)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = fromNCharacterStream(resultSet, columnLabel, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getNCharacterStream(columnLabel);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void fromNCharacterStream_reads_from_null_ncharacter_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnIndex)).thenReturn(null);

        Integer result = fromNCharacterStream(resultSet, columnIndex, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getNCharacterStream(columnIndex);
    }

    @Test
    public void fromNCharacterStream_reads_from_null_ncharacter_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnLabel)).thenReturn(null);

        Integer result = fromNCharacterStream(resultSet, columnLabel, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getNCharacterStream(columnLabel);
    }

    @Test
    public void fromNCharacterStream_throws_InvalidArgumentException_when_stream_reader_parameter_null() {
        assertThrows(() -> fromNCharacterStream(resultSet, columnIndex, null), InvalidArgumentException.class);
        assertThrows(() -> fromNCharacterStream(resultSet, columnLabel, null), InvalidArgumentException.class);
    }

    @Test
    public void fromBlob_reads_from_blob_using_column_label() throws SQLException, IOException {
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(inputStream);
        when(resultSet.getBlob(columnLabel)).thenReturn(blob);

        int expectedValue = 10;
        Integer result = fromBlob(resultSet, columnLabel, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(blob, inputStream, resultSet);
        order.verify(resultSet, times(1)).getBlob(columnLabel);
        order.verify(blob, times(1)).getBinaryStream();
        order.verify(inputStream, times(1)).close();
        order.verify(blob, times(1)).free();
    }

    @Test
    public void fromBlob_reads_from_blob_using_column_index() throws SQLException, IOException {
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(inputStream);
        when(resultSet.getBlob(columnIndex)).thenReturn(blob);

        int expectedValue = 10;
        Integer result = fromBlob(resultSet, columnIndex, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(blob, inputStream, resultSet);
        order.verify(resultSet, times(1)).getBlob(columnIndex);
        order.verify(blob, times(1)).getBinaryStream();
        order.verify(inputStream, times(1)).close();
        order.verify(blob, times(1)).free();
    }

    @Test
    public void fromBlob_reads_from_null_blob_using_column_label() throws SQLException, IOException {
        when(resultSet.getBlob(columnLabel)).thenReturn(null);

        Integer result = fromBlob(resultSet, columnLabel, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getBlob(columnLabel);
    }

    @Test
    public void fromBlob_reads_from_null_blob_using_column_index() throws SQLException, IOException {
        when(resultSet.getBlob(columnIndex)).thenReturn(null);

        Integer result = fromBlob(resultSet, columnIndex, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getBlob(columnIndex);
    }

    @Test
    public void fromBlob_throws_InvalidArgumentException_when_stream_reader_parameter_null() {
        assertThrows(() -> fromBlob(resultSet, columnIndex, null), InvalidArgumentException.class);
        assertThrows(() -> fromBlob(resultSet, columnLabel, null), InvalidArgumentException.class);
    }

    @Test
    public void fromClob_reads_from_clob_using_column_label() throws SQLException, IOException {
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(reader);
        when(resultSet.getClob(columnLabel)).thenReturn(clob);

        int expectedValue = 10;
        Integer result = fromClob(resultSet, columnLabel, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(clob, reader, resultSet);
        order.verify(resultSet, times(1)).getClob(columnLabel);
        order.verify(clob, times(1)).getCharacterStream();
        order.verify(reader, times(1)).close();
        order.verify(clob, times(1)).free();
    }

    @Test
    public void fromClob_reads_from_clob_using_column_index() throws SQLException, IOException {
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(reader);
        when(resultSet.getClob(columnIndex)).thenReturn(clob);

        int expectedValue = 10;
        Integer result = fromClob(resultSet, columnIndex, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(clob, reader, resultSet);
        order.verify(resultSet, times(1)).getClob(columnIndex);
        order.verify(clob, times(1)).getCharacterStream();
        order.verify(reader, times(1)).close();
        order.verify(clob, times(1)).free();
    }

    @Test
    public void fromClob_reads_from_null_clob_using_column_label() throws SQLException, IOException {
        when(resultSet.getClob(columnLabel)).thenReturn(null);

        Integer result = fromClob(resultSet, columnLabel, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getClob(columnLabel);
    }

    @Test
    public void fromClob_reads_from_null_clob_using_column_index() throws SQLException, IOException {
        when(resultSet.getClob(columnIndex)).thenReturn(null);

        Integer result = fromClob(resultSet, columnIndex, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getClob(columnIndex);
    }

    @Test
    public void fromClob_throws_InvalidArgumentException_when_stream_reader_parameter_null() {
        assertThrows(() -> fromClob(resultSet, columnIndex, null), InvalidArgumentException.class);
        assertThrows(() -> fromClob(resultSet, columnLabel, null), InvalidArgumentException.class);
    }

    @Test
    public void asByteArray_reads_byte_arrays() throws SQLException {
        asByteArray(resultSet, columnLabel);
        verify(resultSet, times(1)).getBytes(columnLabel);
        asByteArray(resultSet, columnIndex);
        verify(resultSet, times(1)).getBytes(columnIndex);
    }

    @Test
    public void asString_reads_strings() throws SQLException {
        asString(resultSet, columnLabel);
        verify(resultSet, times(1)).getString(columnLabel);
        asString(resultSet, columnIndex);
        verify(resultSet, times(1)).getString(columnIndex);
    }

    @Test
    public void asShort_reads_valid_shorts() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asShort(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getShort(columnLabel);

        assertThat(asShort(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getShort(columnIndex);
    }

    @Test
    public void asShort_reads_null_shorts() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asShort(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getShort(columnLabel);

        assertThat(asShort(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getShort(columnIndex);
    }

    @Test
    public void asInteger_reads_valid_integers() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asInteger(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getInt(columnLabel);

        assertThat(asInteger(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getInt(columnIndex);
    }

    @Test
    public void asInteger_reads_null_integers() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asInteger(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getInt(columnLabel);

        assertThat(asInteger(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getInt(columnIndex);
    }

    @Test
    public void asLong_reads_valid_longs() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asLong(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getLong(columnLabel);

        assertThat(asLong(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getLong(columnIndex);
    }

    @Test
    public void asLong_reads_null_longs() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asLong(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getLong(columnLabel);

        assertThat(asLong(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getLong(columnIndex);
    }

    @Test
    public void asDouble_reads_valid_doubles() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asDouble(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getDouble(columnLabel);

        assertThat(asDouble(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getDouble(columnIndex);
    }

    @Test
    public void asDouble_reads_null_doubles() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asDouble(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getDouble(columnLabel);

        assertThat(asDouble(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getDouble(columnIndex);
    }

    @Test
    public void asFloat_reads_valid_floats() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asFloat(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getFloat(columnLabel);

        assertThat(asFloat(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getFloat(columnIndex);
    }

    @Test
    public void asFloat_reads_null_floats() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asFloat(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getFloat(columnLabel);

        assertThat(asFloat(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getFloat(columnIndex);
    }

    @Test
    public void asByte_reads_valid_bytes() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asByte(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getByte(columnLabel);

        assertThat(asByte(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getByte(columnIndex);
    }

    @Test
    public void asByte_reads_null_bytes() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asByte(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getByte(columnLabel);

        assertThat(asByte(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getByte(columnIndex);
    }

    @Test
    public void asBigDecimal_reads_bigdecimals() throws SQLException {
        asBigDecimal(resultSet, columnLabel);
        verify(resultSet, times(1)).getBigDecimal(columnLabel);
        asBigDecimal(resultSet, columnIndex);
        verify(resultSet, times(1)).getBigDecimal(columnIndex);
    }

    @Test
    public void asBoolean_reads_valid_booleans() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(asBoolean(resultSet, columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnLabel);

        assertThat(asBoolean(resultSet, columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnIndex);
    }

    @Test
    public void asBoolean_reads_null_booleans() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(asBoolean(resultSet, columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnLabel);

        assertThat(asBoolean(resultSet, columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnIndex);
    }

    @Test
    public void asDate_reads_dates() throws SQLException {
        asDate(resultSet, columnLabel);
        verify(resultSet, times(1)).getDate(columnLabel);
        asDate(resultSet, columnIndex);
        verify(resultSet, times(1)).getDate(columnIndex);
    }

    @Test
    public void asTime_reads_times() throws SQLException {
        asTime(resultSet, columnLabel);
        verify(resultSet, times(1)).getTime(columnLabel);
        asTime(resultSet, columnIndex);
        verify(resultSet, times(1)).getTime(columnIndex);
    }

    @Test
    public void asTimestamp_reads_timestamps() throws SQLException {
        asTimestamp(resultSet, columnLabel);
        verify(resultSet, times(1)).getTimestamp(columnLabel);
        asTimestamp(resultSet, columnIndex);
        verify(resultSet, times(1)).getTimestamp(columnIndex);
    }

    private Function<InputStream, Integer> makeFunctionAcceptingOnlyNullInputStream() {
        return inputStream -> {
            if (inputStream == null) {
                return null;
            }
            throw new RuntimeException("Shouldn't be here");
        };
    }

    private Function<Reader, Integer> makeFunctionAcceptingOnlyNullReader() {
        return reader -> {
            if (reader == null) {
                return null;
            }
            throw new RuntimeException("Shouldn't be here");
        };
    }

    private Function<InputStream, Integer> makeFunctionWitResultOrThrowIfInputStreamNull(int expectedValue) {
        return inputStream -> {
            if (inputStream != null) {
                return expectedValue;
            }
            throw new RuntimeException("Shouldn't be here");
        };
    }

    private Function<Reader, Integer> makeFunctionWitResultOrThrowIfReaderNull(int expectedValue) {
        return reader -> {
            if (reader != null) {
                return expectedValue;
            }
            throw new RuntimeException("Shouldn't be here");
        };
    }

    private <T extends RuntimeException> void assertThrows(CodeBlock codeBlock, Class<T> exceptionClass) {
        try {
            codeBlock.execute();
            fail("Exception expected");
        } catch (RuntimeException exception) {
            assertTrue(exception.getClass().equals(exceptionClass));
        }
    }

    @FunctionalInterface
    private interface CodeBlock {
        void execute();
    }
}