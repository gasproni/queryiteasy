package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.InputParameterBinders;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;

import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
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

        wrappedJDBCConnection.update(sql, InputParameterBinders.bindBlob(() -> blobStream));

        InOrder order = inOrder(preparedStatement, blobStream);
        order.verify(preparedStatement, times(1)).execute();
        order.verify(blobStream, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void result_stream_closes_result_set_and_statement_in_the_right_order() throws Exception {
        String sql = "SELECT * FROM foo";
        PreparedStatement preparedStatement = prepareStatement(sql);

        ResultSet rs = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        wrappedJDBCConnection.select(row -> row.at(1), sql).close();

        InOrder order = inOrder(preparedStatement, rs);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(rs, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void select_closes_blob_streams_after_executing_query() throws Exception {
        String sql = "SELECT * FROM foo where blob = ?";
        PreparedStatement preparedStatement = prepareStatement(sql);

        InputStream blobStream = mock(InputStream.class);

        ResultSet rs = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        wrappedJDBCConnection.select(row -> row.at(1), sql, InputParameterBinders.bindBlob(() -> blobStream));

        InOrder order = inOrder(preparedStatement, rs, blobStream);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(blobStream, times(1)).close();
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

        wrappedJDBCConnection.update(sql, Collections.singletonList(batch(InputParameterBinders.bindBlob(() -> blobStream))));

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