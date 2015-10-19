package com.asprotunity.queryiteasy;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class TransactionExecutorTest {


    private java.sql.Connection jdbcConnection;
    private TransactionExecutor transactionExecutor;

    @Before
    public void setUp() throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        jdbcConnection = Mockito.mock(java.sql.Connection.class);
        when(dataSource.getConnection()).thenReturn(jdbcConnection);
        transactionExecutor = new TransactionExecutor(dataSource);

    }

    @Test
    public void commits_rollbacks_and_closes_transaction_in_this_order() throws java.sql.SQLException {

        transactionExecutor.executeUpdate(connection -> {
        });

        InOrder order = inOrder(jdbcConnection);
        order.verify(jdbcConnection, times(1)).commit();
        order.verify(jdbcConnection, times(1)).rollback();
        order.verify(jdbcConnection, times(1)).close();
    }


    @Test
    public void when_exception_thrown_rollbacks_and_closes_transaction_in_this_order_and_doesnt_commit() throws java.sql.SQLException {

        try {
            transactionExecutor.executeUpdate(connection -> {
                throw new RuntimeException();
            });
            fail("Runtime exception expected");
        } catch (RuntimeException ignored) {
            InOrder order = inOrder(jdbcConnection);
            order.verify(jdbcConnection, times(1)).rollback();
            order.verify(jdbcConnection, times(1)).close();
            order.verify(jdbcConnection, times(0)).commit();
        }
    }


    @Test
    public void returns_correct_query_result_primitive_type() throws java.sql.SQLException {

        final int result = 10;

        int queryResult = transactionExecutor.executeQuery(connection -> result);

        assertThat(queryResult, is(result));
    }

    @Test
    public void returns_correct_query_result_list() throws java.sql.SQLException {

        final List<Integer> result = Arrays.asList(1, 2, 3, 4);

        List<Integer> queryResult = transactionExecutor.executeQuery(connection -> result);

        assertThat(queryResult, is(result));
    }
}
