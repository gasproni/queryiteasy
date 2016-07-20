package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.*;

public class RowFromResultSetTest {

    private final ResultSet resultSet = mock(ResultSet.class);
    private final int columnIndex = 5;
    private final String columnLabel = "columnLabel";
    private final Row row = new RowFromResultSet(resultSet);

    @Test
    public void reads_from_binary_stream_using_column_index() throws SQLException, IOException {
        InputStream binaryStream = mock(InputStream.class);
        when(resultSet.getBinaryStream(columnIndex)).thenReturn(binaryStream);

        int expectedValue = 10;
        Integer result = row.fromBinaryStream(columnIndex, inputStream -> {
            if (inputStream != null) {
                return expectedValue;
            }
            throw new RuntimeException("Shouldn't be here");
        });

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(binaryStream, resultSet);
        order.verify(resultSet, times(1)).getBinaryStream(columnIndex);
        order.verify(binaryStream, times(1)).close();
    }

    @Test
    public void reads_from_binary_stream_using_column_label() throws SQLException, IOException {
        InputStream binaryStream = mock(InputStream.class);
        when(resultSet.getBinaryStream(columnLabel)).thenReturn(binaryStream);

        int expectedValue = 10;
        Integer result = row.fromBinaryStream(columnLabel, inputStream -> {
            if (inputStream != null) {
                return expectedValue;
            }
            throw new RuntimeException("Shouldn't be here");
        });

        assertThat(result, is(expectedValue));
        InOrder order = inOrder(binaryStream, resultSet);
        order.verify(resultSet, times(1)).getBinaryStream(columnLabel);
        order.verify(binaryStream, times(1)).close();
    }

    @Test
    public void reads_from_null_binary_stream_using_column_index() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnIndex)).thenReturn(null);

        Integer result = row.fromBinaryStream(columnIndex, inputStream -> {
            if (inputStream == null) {
                return null;
            }
            throw new RuntimeException("Shouldn't be here");
        });

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getBinaryStream(columnIndex);
    }

    @Test
    public void reads_from_null_binary_stream_using_column_label() throws SQLException, IOException {
        when(resultSet.getBinaryStream(columnLabel)).thenReturn(null);

        Integer result = row.fromBinaryStream(columnLabel, inputStream -> {
            if (inputStream == null) {
                return null;
            }
            throw new RuntimeException("Shouldn't be here");
        });

        assertThat(result, is(nullValue()));
        verify(resultSet, times(1)).getBinaryStream(columnLabel);
    }
}