package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.GenericRow;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.AutoCloseableScope;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;

import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bindBlob;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class WrappedJDBCConnectionTest {

    private Connection jdbcConnection = mock(Connection.class);
    private AutoCloseableScope connectionScope = mock(AutoCloseableScope.class);
    private WrappedJDBCConnection<Row> wrappedJDBCConnection =
            new WrappedJDBCConnection<>(jdbcConnection, connectionScope, GenericRow::new);

    @Before
    public void setUp() {
        when(connectionScope.add(any(Object.class), any())).thenAnswer(invocation -> invocation.getArguments()[0]);
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
        InOrder order = inOrder(jdbcConnection, connectionScope);
        order.verify(jdbcConnection, times(1)).rollback();
        order.verify(connectionScope, times(1)).close();
        order.verify(jdbcConnection, times(1)).close();
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
    public void update_closes_query_scope_before_closing_statement() throws Exception {
        String sql = "INSERT INTO foo VALUES(?)";
        PreparedStatement preparedStatement = prepareStatement(sql);
        InputStream blobStream = mock(InputStream.class);

        wrappedJDBCConnection.update(sql, bindBlob(() -> blobStream));

        InOrder order = inOrder(preparedStatement, blobStream);
        order.verify(preparedStatement, times(1)).execute();
        order.verify(blobStream, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void result_stream_closes_result_set_and_statement_in_the_right_order() throws Exception {
        String sql = "SELECT * FROM foo";
        PreparedStatement preparedStatement = prepareStatement(sql);

        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        wrappedJDBCConnection.select(row -> row.asInteger(1), sql).close();

        InOrder order = inOrder(preparedStatement, resultSet);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(resultSet, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void select_closes_query_scope_but_leaves_resultset_and_statement_scope_open_after_executing_query() throws Exception {
        String sql = "SELECT * FROM foo where blob = ?";
        PreparedStatement preparedStatement = prepareStatement(sql);

        InputStream blobStream = mock(InputStream.class);

        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        wrappedJDBCConnection.select(row -> row.asInteger(1), sql, bindBlob(() -> blobStream));

        InOrder order = inOrder(preparedStatement, resultSet, blobStream);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(blobStream, times(1)).close();

        verify(resultSet, times(0)).close();
        verify(preparedStatement, times(0)).close();
    }

    @Test
    public void select_closes_query_scope_if_query_throws_exception() throws Exception {
        String sql = "SELECT * FROM foo where blob = ?";
        PreparedStatement preparedStatement = prepareStatement(sql);

        InputStream blobStream = mock(InputStream.class);

        when(preparedStatement.executeQuery()).thenThrow(new SQLException());

        try {
            wrappedJDBCConnection.select(row -> row.asInteger(1), sql, bindBlob(() -> blobStream));
            fail("RuntimeSQLException expected");
        }
        catch (RuntimeSQLException exception) {
            InOrder order = inOrder(preparedStatement, blobStream);
            order.verify(preparedStatement, times(1)).executeQuery();
            order.verify(blobStream, times(1)).close();
            verify(preparedStatement, times(1)).close();
        }
    }

    @Test
    public void call_with_results_closes_query_scope_but_leaves_resultset_and_statement_scope_open_after_executing_query() throws Exception {
        String sql = "{call foo_func(?)}";
        CallableStatement callableStatement = prepareCall(sql);

        InputStream blobStream = mock(InputStream.class);

        ResultSet resultSet = mock(ResultSet.class);
        when(callableStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        wrappedJDBCConnection.call(row -> row.asInteger(1), sql, bindBlob(() -> blobStream));

        InOrder order = inOrder(callableStatement, resultSet, blobStream);
        order.verify(callableStatement, times(1)).executeQuery();
        order.verify(blobStream, times(1)).close();

        verify(resultSet, times(0)).close();
        verify(callableStatement, times(0)).close();
    }

    @Test
    public void call_with_result_closes_query_scope_and_resultset_and_statement_scope_if_query_throws_exception() throws Exception {
        String sql = "{call foo_func(?)}";
        CallableStatement callableStatement = prepareCall(sql);

        InputStream blobStream = mock(InputStream.class);

        when(callableStatement.executeQuery()).thenThrow(new SQLException());

        try {
            wrappedJDBCConnection.call(row -> row.asInteger(1), sql, bindBlob(() -> blobStream));
            fail("RuntimeSQLException expected");
        }
        catch (RuntimeSQLException exception) {
            InOrder order = inOrder(callableStatement, blobStream);
            order.verify(callableStatement, times(1)).executeQuery();
            order.verify(blobStream, times(1)).close();
            verify(callableStatement, times(1)).close();
        }
    }

    @Test
    public void call_with_no_results_closes_query_scope_and_statement() throws Exception {
        String sql = "{call foo_func(?)}";

        CallableStatement callableStatement = prepareCall(sql);

        InputStream blobStream = mock(InputStream.class);

        wrappedJDBCConnection.call(sql, bindBlob(() -> blobStream));

        InOrder order = inOrder(callableStatement, blobStream);
        order.verify(callableStatement, times(1)).execute();
        order.verify(blobStream, times(1)).close();
        verify(callableStatement, times(1)).close();
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

        wrappedJDBCConnection.update(sql, Collections.singletonList(batch(bindBlob(() -> blobStream))));

        InOrder order = inOrder(preparedStatement, blobStream);
        order.verify(preparedStatement, times(1)).executeBatch();
        order.verify(blobStream, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    private PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement result = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(sql)).thenReturn(result);
        return result;
    }

    private CallableStatement prepareCall(String sql) throws SQLException {
        CallableStatement result = mock(CallableStatement.class);
        when(jdbcConnection.prepareCall(sql)).thenReturn(result);
        return result;
    }

}