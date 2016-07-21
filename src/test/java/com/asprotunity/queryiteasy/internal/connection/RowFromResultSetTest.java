package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Mockito.*;

public class RowFromResultSetTest {

    private final int columnIndex = 5;
    private final String columnLabel = "columnLabel";
    private final ResultSet resultSet = mock(ResultSet.class);
    private final Row row = new RowFromResultSet(resultSet);
    private InputStream inputStream = mock(InputStream.class);
    private Reader reader = mock(Reader.class);

    @Test
    public void reads_from_binary_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnIndex)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = row.fromBinaryStream(columnIndex, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getBinaryStream(columnIndex);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void reads_from_binary_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnLabel)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = row.fromBinaryStream(columnLabel, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getBinaryStream(columnLabel);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void reads_from_null_binary_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnIndex)).thenReturn(null);

        Integer result = row.fromBinaryStream(columnIndex, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getBinaryStream(columnIndex);
    }

    @Test
    public void reads_from_null_binary_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnLabel)).thenReturn(null);

        Integer result = row.fromBinaryStream(columnLabel, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getBinaryStream(columnLabel);
    }

    @Test
    public void reads_from_ascii_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnIndex)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = row.fromAsciiStream(columnIndex, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getAsciiStream(columnIndex);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void reads_from_ascii_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnLabel)).thenReturn(inputStream);

        int expectedValue = 10;
        Integer result = row.fromAsciiStream(columnLabel, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(inputStream, resultSet);
        order.verify(resultSet, times(1)).getAsciiStream(columnLabel);
        order.verify(inputStream, times(1)).close();
    }

    @Test
    public void reads_from_null_ascii_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnIndex)).thenReturn(null);

        Integer result = row.fromAsciiStream(columnIndex, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getAsciiStream(columnIndex);
    }

    @Test
    public void reads_from_null_ascii_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getAsciiStream(columnLabel)).thenReturn(null);

        Integer result = row.fromAsciiStream(columnLabel, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getAsciiStream(columnLabel);
    }

    @Test
    public void reads_from_character_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnIndex)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = row.fromCharacterStream(columnIndex, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getCharacterStream(columnIndex);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void reads_from_character_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnLabel)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = row.fromCharacterStream(columnLabel, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getCharacterStream(columnLabel);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void reads_from_null_character_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnIndex)).thenReturn(null);

        Integer result = row.fromCharacterStream(columnIndex, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getCharacterStream(columnIndex);
    }

    @Test
    public void reads_from_null_character_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getCharacterStream(columnLabel)).thenReturn(null);

        Integer result = row.fromCharacterStream(columnLabel, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getCharacterStream(columnLabel);
    }

    @Test
    public void reads_from_ncharacter_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnIndex)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = row.fromNCharacterStream(columnIndex, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getNCharacterStream(columnIndex);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void reads_from_ncharacter_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnLabel)).thenReturn(reader);

        int expectedValue = 10;
        Integer result = row.fromNCharacterStream(columnLabel, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(reader, resultSet);
        order.verify(resultSet, times(1)).getNCharacterStream(columnLabel);
        order.verify(reader, times(1)).close();
    }

    @Test
    public void reads_from_null_ncharacter_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnIndex)).thenReturn(null);

        Integer result = row.fromNCharacterStream(columnIndex, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getNCharacterStream(columnIndex);
    }

    @Test
    public void reads_from_null_ncharacter_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getNCharacterStream(columnLabel)).thenReturn(null);

        Integer result = row.fromNCharacterStream(columnLabel, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getNCharacterStream(columnLabel);
    }

    @Test
    public void reads_from_blob_using_column_label() throws SQLException, IOException {
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(inputStream);
        when(resultSet.getBlob(columnLabel)).thenReturn(blob);

        int expectedValue = 10;
        Integer result = row.fromBlob(columnLabel, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(blob, inputStream, resultSet);
        order.verify(resultSet, times(1)).getBlob(columnLabel);
        order.verify(blob, times(1)).getBinaryStream();
        order.verify(inputStream, times(1)).close();
        order.verify(blob, times(1)).free();
    }

    @Test
    public void reads_from_blob_using_column_index() throws SQLException, IOException {
        Blob blob = mock(Blob.class);
        when(blob.getBinaryStream()).thenReturn(inputStream);
        when(resultSet.getBlob(columnIndex)).thenReturn(blob);

        int expectedValue = 10;
        Integer result = row.fromBlob(columnIndex, makeFunctionWitResultOrThrowIfInputStreamNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(blob, inputStream, resultSet);
        order.verify(resultSet, times(1)).getBlob(columnIndex);
        order.verify(blob, times(1)).getBinaryStream();
        order.verify(inputStream, times(1)).close();
        order.verify(blob, times(1)).free();
    }

    @Test
    public void reads_from_null_blob_using_column_label() throws SQLException, IOException {
        when(resultSet.getBlob(columnLabel)).thenReturn(null);

        Integer result = row.fromBlob(columnLabel, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getBlob(columnLabel);
    }

    @Test
    public void reads_from_null_blob_using_column_index() throws SQLException, IOException {
        when(resultSet.getBlob(columnIndex)).thenReturn(null);

        Integer result = row.fromBlob(columnIndex, makeFunctionAcceptingOnlyNullInputStream());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getBlob(columnIndex);
    }

    @Test
    public void reads_from_clob_using_column_label() throws SQLException, IOException {
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(reader);
        when(resultSet.getClob(columnLabel)).thenReturn(clob);

        int expectedValue = 10;
        Integer result = row.fromClob(columnLabel, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(clob, reader, resultSet);
        order.verify(resultSet, times(1)).getClob(columnLabel);
        order.verify(clob, times(1)).getCharacterStream();
        order.verify(reader, times(1)).close();
        order.verify(clob, times(1)).free();
    }

    @Test
    public void reads_from_clob_using_column_index() throws SQLException, IOException {
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(reader);
        when(resultSet.getClob(columnIndex)).thenReturn(clob);

        int expectedValue = 10;
        Integer result = row.fromClob(columnIndex, makeFunctionWitResultOrThrowIfReaderNull(expectedValue));

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(clob, reader, resultSet);
        order.verify(resultSet, times(1)).getClob(columnIndex);
        order.verify(clob, times(1)).getCharacterStream();
        order.verify(reader, times(1)).close();
        order.verify(clob, times(1)).free();
    }

    @Test
    public void reads_from_null_clob_using_column_label() throws SQLException, IOException {
        when(resultSet.getClob(columnLabel)).thenReturn(null);

        Integer result = row.fromClob(columnLabel, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getClob(columnLabel);
    }

    @Test
    public void reads_from_null_clob_using_column_index() throws SQLException, IOException {
        when(resultSet.getClob(columnIndex)).thenReturn(null);

        Integer result = row.fromClob(columnIndex, makeFunctionAcceptingOnlyNullReader());

        assertThat(result, is(nullValue()));
        InOrder order = inOrder(resultSet);
        order.verify(resultSet, times(1)).getClob(columnIndex);
    }

    @Test
    public void reads_byte_arrays() throws SQLException {
        row.asByteArray(columnLabel);
        verify(resultSet, times(1)).getBytes(columnLabel);
        row.asByteArray(columnIndex);
        verify(resultSet, times(1)).getBytes(columnIndex);
    }

    @Test
    public void reads_strings() throws SQLException {
        row.asString(columnLabel);
        verify(resultSet, times(1)).getString(columnLabel);
        row.asString(columnIndex);
        verify(resultSet, times(1)).getString(columnIndex);
    }

    @Test
    public void reads_valid_shorts() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asShort(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getShort(columnLabel);

        assertThat(row.asShort(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getShort(columnIndex);
    }

    @Test
    public void reads_null_shorts() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asShort(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getShort(columnLabel);

        assertThat(row.asShort(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getShort(columnIndex);
    }

    @Test
    public void reads_valid_integers() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asInteger(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getInt(columnLabel);

        assertThat(row.asInteger(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getInt(columnIndex);
    }

    @Test
    public void reads_null_integers() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asInteger(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getInt(columnLabel);

        assertThat(row.asInteger(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getInt(columnIndex);
    }

    @Test
    public void reads_valid_longs() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asLong(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getLong(columnLabel);

        assertThat(row.asLong(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getLong(columnIndex);
    }

    @Test
    public void reads_null_longs() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asLong(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getLong(columnLabel);

        assertThat(row.asLong(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getLong(columnIndex);
    }

    @Test
    public void reads_valid_doubles() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asDouble(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getDouble(columnLabel);

        assertThat(row.asDouble(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getDouble(columnIndex);
    }

    @Test
    public void reads_null_doubles() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asDouble(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getDouble(columnLabel);

        assertThat(row.asDouble(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getDouble(columnIndex);
    }

    @Test
    public void reads_valid_floats() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asFloat(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getFloat(columnLabel);

        assertThat(row.asFloat(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getFloat(columnIndex);
    }

    @Test
    public void reads_null_floats() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asFloat(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getFloat(columnLabel);

        assertThat(row.asFloat(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getFloat(columnIndex);
    }

    @Test
    public void reads_valid_bytes() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asByte(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getByte(columnLabel);

        assertThat(row.asByte(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getByte(columnIndex);
    }

    @Test
    public void reads_null_bytes() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asByte(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getByte(columnLabel);

        assertThat(row.asByte(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getByte(columnIndex);
    }

    @Test
    public void reads_bigdecimals() throws SQLException {
        row.asBigDecimal(columnLabel);
        verify(resultSet, times(1)).getBigDecimal(columnLabel);
        row.asBigDecimal(columnIndex);
        verify(resultSet, times(1)).getBigDecimal(columnIndex);
    }

    @Test
    public void reads_valid_booleans() throws SQLException {
        when(resultSet.wasNull()).thenReturn(false);

        assertThat(row.asBoolean(columnLabel), not(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnLabel);

        assertThat(row.asBoolean(columnIndex), not(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnIndex);
    }

    @Test
    public void reads_null_booleans() throws SQLException {
        when(resultSet.wasNull()).thenReturn(true);

        assertThat(row.asBoolean(columnLabel), is(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnLabel);

        assertThat(row.asBoolean(columnIndex), is(nullValue()));
        verify(resultSet, times(1)).getBoolean(columnIndex);
    }

    @Test
    public void reads_dates() throws SQLException {
        row.asDate(columnLabel);
        verify(resultSet, times(1)).getDate(columnLabel);
        row.asDate(columnIndex);
        verify(resultSet, times(1)).getDate(columnIndex);
    }

    @Test
    public void reads_times() throws SQLException {
        row.asTime(columnLabel);
        verify(resultSet, times(1)).getTime(columnLabel);
        row.asTime(columnIndex);
        verify(resultSet, times(1)).getTime(columnIndex);
    }

    @Test
    public void reads_timestamps() throws SQLException {
        row.asTimestamp(columnLabel);
        verify(resultSet, times(1)).getTimestamp(columnLabel);
        row.asTimestamp(columnIndex);
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
}