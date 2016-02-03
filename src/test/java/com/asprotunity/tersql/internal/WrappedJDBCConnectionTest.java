package com.asprotunity.tersql.internal;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.stream.Stream;

import static com.asprotunity.tersql.connection.Batch.batch;
import static com.asprotunity.tersql.connection.StatementParameter.bind;
import static org.mockito.Mockito.*;

public class WrappedJDBCConnectionTest {

    private Connection jdbcConnection;
    private WrappedJDBCConnection wrappedJDBCConnection;

    @Before
    public void setUp() {
        jdbcConnection = mock(Connection.class);
        wrappedJDBCConnection = new WrappedJDBCConnection(jdbcConnection);
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
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(sql)).thenReturn(preparedStatement);

        wrappedJDBCConnection.update(sql);

        InOrder order = inOrder(preparedStatement);
        order.verify(preparedStatement, times(1)).execute();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void select_executes_and_closes_result_set_and_statement_in_the_right_order() throws Exception {
        String sql = "SELECT * FROM foo";
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(sql)).thenReturn(preparedStatement);
        ResultSet rs = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        wrappedJDBCConnection.select(sql, Stream::count);

        InOrder order = inOrder(preparedStatement, rs);
        order.verify(preparedStatement, times(1)).executeQuery();
        order.verify(rs, times(1)).close();
        order.verify(preparedStatement, times(1)).close();
    }

    @Test
    public void batch_update_executes_batch_and_closes_statement_correctly() throws Exception {
        String sql = "INSERT INTO foo VALUES(?)";
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(jdbcConnection.prepareStatement(sql)).thenReturn(preparedStatement);

        wrappedJDBCConnection.update(sql, batch(bind(10)), batch(bind(20)));

        InOrder order = inOrder(preparedStatement);
        order.verify(preparedStatement, times(1)).setObject(1, 10, Types.INTEGER);
        order.verify(preparedStatement, times(1)).addBatch();
        order.verify(preparedStatement, times(1)).setObject(1, 20, Types.INTEGER);
        order.verify(preparedStatement, times(1)).addBatch();
        order.verify(preparedStatement, times(1)).executeBatch();
        order.verify(preparedStatement, times(1)).close();
    }

}