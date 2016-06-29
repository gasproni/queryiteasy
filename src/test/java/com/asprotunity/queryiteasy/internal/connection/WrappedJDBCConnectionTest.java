package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class WrappedJDBCConnectionTest {

    private Connection jdbcConnection;
    private WrappedJDBCConnection wrappedJDBCConnection;

    @Before
    public void setUp() {
        jdbcConnection = mock(Connection.class);
        wrappedJDBCConnection = new WrappedJDBCConnection(jdbcConnection,
                WrappedJDBCResultSet::new);
    }

    @Test
    public void sets_jdbc_autocommit_to_false() throws Exception {
        verify(jdbcConnection, times(1)).setAutoCommit(false);
    }

    @Test
    public void commits_jdbc_connection_correctly() throws Exception {
        wrappedJDBCConnection.commit();
        verify(jdbcConnection, times(1)).commit();
    }

    @Test
    public void closes_jdbc_connection_correctly() throws Exception {
        wrappedJDBCConnection.close();
        verify(jdbcConnection, times(1)).close();
    }

    @Test
    public void update_executes_and_closes_statement_in_the_right_order() throws Exception {
        String sql = "INSERT INTO foo VALUES(1)";
        PreparedStatement preparedStatement = prepareStatement(sql);

        wrappedJDBCConnection.update(sql);

        InOrder order = inOrder(preparedStatement);
        order.verify(preparedStatement, times(1)).execute();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void update_closes_blob_stream_before_closing_statement() throws Exception {
        String sql = "INSERT INTO foo VALUES(?)";
        PreparedStatement preparedStatement = prepareStatement(sql);
        InputStream blobStream = mock(InputStream.class);

        wrappedJDBCConnection.update(sql, bind(() -> blobStream));

        InOrder order = inOrder(preparedStatement, blobStream);
        order.verify(preparedStatement, times(1)).execute();
        order.verify(blobStream, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void select_executes_and_closes_result_set_stream_and_statement_in_the_right_order() throws Exception {
        String sql = "SELECT * FROM foo";
        PreparedStatement preparedStatement = prepareStatement(sql);

        ResultSet rs = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        final AtomicBoolean streamClosed = new AtomicBoolean(false);
        Function<Stream<Row>, Stream<Row>> setStreamOnCloseToVerifyResultSetNotClosedBeforeStreamAndMarkStreamClosed =
                rowStream -> {
                    rowStream.onClose(() -> {
                        try {
                            verify(rs, times(0)).close();
                            streamClosed.set(true);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    return rowStream;
                };

        wrappedJDBCConnection.select(sql, setStreamOnCloseToVerifyResultSetNotClosedBeforeStreamAndMarkStreamClosed);

        assertTrue(streamClosed.get());
        InOrder order = inOrder(preparedStatement, rs);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(rs, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void select_closes_blob_streams_before_closing_statement() throws Exception {
        String sql = "SELECT * FROM foo where blob = ?";
        PreparedStatement preparedStatement = prepareStatement(sql);

        InputStream blobStream = mock(InputStream.class);

        ResultSet rs = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        wrappedJDBCConnection.select(sql, rowStream -> 1, bind(() -> blobStream));

        InOrder order = inOrder(preparedStatement, rs, blobStream);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(rs, times(1)).close();
        order.verify(blobStream, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void closes_selected_blobs_on_close() throws Exception {
        String sql = "SELECT * FROM foo";

        Blob blob = mock(Blob.class);
        ResultSetWrapperFactory resultSetWrapperFactory = makeResultSetWrapperFactory(sql, blob);

        wrappedJDBCConnection = new WrappedJDBCConnection(jdbcConnection, resultSetWrapperFactory);

        wrappedJDBCConnection.select(sql, rowStream -> rowStream.collect(Collectors.toList()));
        wrappedJDBCConnection.close();
        InOrder order = inOrder(blob, jdbcConnection);
        order.verify(blob, times(1)).free();
        order.verify(jdbcConnection, times(1)).close();
    }

    @Test
    public void closes_selected_nclobs_on_close() throws Exception {
        String sql = "SELECT * FROM foo";

        NClob nclob = mock(NClob.class);
        ResultSetWrapperFactory resultSetWrapperFactory = makeResultSetWrapperFactory(sql, nclob);

        wrappedJDBCConnection = new WrappedJDBCConnection(jdbcConnection, resultSetWrapperFactory);

        wrappedJDBCConnection.select(sql, rowStream -> rowStream.collect(Collectors.toList()));
        wrappedJDBCConnection.close();
        InOrder order = inOrder(nclob, jdbcConnection);
        order.verify(nclob, times(1)).free();
        order.verify(jdbcConnection, times(1)).close();
    }

    @Test
    public void closes_selected_clobs_on_close() throws Exception {
        String sql = "SELECT * FROM foo";

        Clob clob = mock(Clob.class);
        ResultSetWrapperFactory resultSetWrapperFactory = makeResultSetWrapperFactory(sql, clob);

        wrappedJDBCConnection = new WrappedJDBCConnection(jdbcConnection, resultSetWrapperFactory);

        wrappedJDBCConnection.select(sql, rowStream -> rowStream.collect(Collectors.toList()));
        wrappedJDBCConnection.close();
        InOrder order = inOrder(clob, jdbcConnection);
        order.verify(clob, times(1)).free();
        order.verify(jdbcConnection, times(1)).close();
    }

    @Test
    public void batch_update_executes_batch_and_closes_statement_correctly() throws Exception {
        String sql = "INSERT INTO foo VALUES(?)";
        PreparedStatement preparedStatement = prepareStatement(sql);

        wrappedJDBCConnection.update(sql, Arrays.asList(batch(bind(10)), batch(bind(20))));

        InOrder order = inOrder(preparedStatement);
        order.verify(preparedStatement, times(1)).setObject(1, 10, Types.INTEGER);
        order.verify(preparedStatement, times(1)).addBatch();
        order.verify(preparedStatement, times(1)).setObject(1, 20, Types.INTEGER);
        order.verify(preparedStatement, times(1)).addBatch();
        order.verify(preparedStatement, times(1)).executeBatch();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void batch_update_closes_blob_stream_before_closing_statement() throws Exception {
        String sql = "INSERT INTO foo VALUES(?)";
        PreparedStatement preparedStatement = prepareStatement(sql);

        InputStream blobStream = mock(InputStream.class);

        wrappedJDBCConnection.update(sql, Collections.singletonList(batch(bind(() -> blobStream))));

        InOrder order = inOrder(preparedStatement, blobStream);
        order.verify(preparedStatement, times(1)).executeBatch();
        order.verify(blobStream, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    private PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(sql)).thenReturn(preparedStatement);
        return preparedStatement;
    }

    private ResultSetWrapper makeResultSetWrapperWithOneRowAndOneColumn(Object columnValue) {
        ResultSetWrapper result = mock(ResultSetWrapper.class);
        when(result.columnLabel(1)).thenReturn("colName");
        when(result.columnCount()).thenReturn(1);
        when(result.next()).thenReturn(true, false);
        when(result.getObject(1)).thenReturn(columnValue);
        return result;
    }

    private ResultSetWrapperFactory makeResultSetWrapperFactory(String sql, Object columnValue) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        PreparedStatement preparedStatement = prepareStatement(sql);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        ResultSetWrapper resultSetWrapper = makeResultSetWrapperWithOneRowAndOneColumn(columnValue);

        ResultSetWrapperFactory resultSetWrapperFactory = mock(ResultSetWrapperFactory.class);

        when(resultSetWrapperFactory.make(resultSet)).thenReturn(resultSetWrapper);
        return resultSetWrapperFactory;
    }
}