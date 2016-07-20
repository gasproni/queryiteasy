package com.asprotunity.queryiteasy;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
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
import static org.mockito.Mockito.*;

public class DefaultDataStoreTest {


    private java.sql.Connection jdbcConnection;
    private DefaultDataStore dataStore;

    @Before
    public void setUp() throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        jdbcConnection = Mockito.mock(java.sql.Connection.class);
        when(dataSource.getConnection()).thenReturn(jdbcConnection);
        dataStore = new DefaultDataStore(dataSource);
    }


    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_datasource_is_null() {
        new DefaultDataStore(null);
    }

    @Test
    public void commits_rollbacks_and_closes_transaction_in_this_order() throws java.sql.SQLException {
        dataStore.execute(connection -> {
        });
        assertCommitRollbackAndCloseCalledInThisOrder();
    }


    @Test
    public void commits_rollbacks_and_closes_transaction_in_this_order_for_query() throws java.sql.SQLException {
        dataStore.executeWithResult(connection -> 1);
        assertCommitRollbackAndCloseCalledInThisOrder();
    }

    @Test
    public void when_exception_thrown_rollbacks_and_closes_transaction_in_this_order_and_doesnt_commit()
            throws java.sql.SQLException {
        assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalled(() -> dataStore.execute(connection -> {
            throw new RuntimeException();
        }));
    }

    @Test
    public void when_exception_thrown_rollbacks_and_closes_transaction_in_this_order_and_doesnt_commit_query()
            throws java.sql.SQLException {
        assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalled(() -> dataStore.executeWithResult(connection -> {
            throw new RuntimeException();
        }));

    }

    @Test
    public void returns_correct_query_result_primitive_type() throws java.sql.SQLException {

        final int result = 10;

        int queryResult = dataStore.executeWithResult(connection -> result);

        assertThat(queryResult, is(result));
    }

    @Test
    public void returns_correct_query_result_class_type() throws java.sql.SQLException {

        final ArrayList<Integer> result = new ArrayList<>(Arrays.asList(1, 2, 3, 4));

        List<Integer> queryResult = dataStore.executeWithResult(connection -> result);

        assertThat(queryResult, is(result));
    }

    private void assertCommitRollbackAndCloseCalledInThisOrder() throws SQLException {
        InOrder order = inOrder(jdbcConnection);
        order.verify(jdbcConnection, times(1)).commit();
        order.verify(jdbcConnection, times(1)).rollback();
        order.verify(jdbcConnection, times(1)).close();
    }

    private void assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalled(VoidCodeBlock codeBlock) throws SQLException {
        try {
            codeBlock.execute();
        } catch (Exception ignored) {
        }

        verify(jdbcConnection, times(0)).commit();
        InOrder order = inOrder(jdbcConnection);
        order.verify(jdbcConnection, times(1)).rollback();
        order.verify(jdbcConnection, times(1)).close();
    }

    @FunctionalInterface
    public interface VoidCodeBlock {
        void execute();
    }
}
