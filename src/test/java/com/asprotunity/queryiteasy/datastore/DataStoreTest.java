package com.asprotunity.queryiteasy.datastore;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class DataStoreTest {


    private java.sql.Connection jdbcConnection;
    private DataStore dataStore;

    @Before
    public void setUp() throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        jdbcConnection = Mockito.mock(java.sql.Connection.class);
        when(dataSource.getConnection()).thenReturn(jdbcConnection);
        dataStore = new DataStore(dataSource);
    }


    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_datasource_is_null() {
        new DataStore(null);
    }

    @Test
    public void commits_rollbacks_and_closes_transaction_in_this_order() throws java.sql.SQLException {
        dataStore.execute(connection -> {
        });
        assertCommitRollbackAndCloseCalledInThisOrder();
    }

    @Test(expected = InvalidArgumentException.class)
    public void executeWithResult_throws_when_transaction_is_null() {
        dataStore.executeWithResult(null);
    }

    @Test(expected = InvalidArgumentException.class)
    public void execute_throws_when_transaction_is_null() {
        dataStore.execute(null);
    }

    @Test
    public void commits_rollbacks_and_closes_transaction_in_this_order_for_query() throws java.sql.SQLException {
        dataStore.executeWithResult(connection -> 1);
        assertCommitRollbackAndCloseCalledInThisOrder();
    }

    @Test
    public void when_exception_thrown_rollbacks_and_closes_transaction_in_this_order_and_doesnt_commit()
            throws java.sql.SQLException {
        assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalledWhenExceptionThrown(() -> dataStore.execute(connection -> {
            throw new RuntimeException();
        }));
    }

    @Test
    public void when_exception_thrown_rollbacks_and_closes_transaction_in_this_order_and_doesnt_commit_query()
            throws java.sql.SQLException {
        assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalledWhenExceptionThrown(() -> dataStore.executeWithResult(connection -> {
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

    @Test
    public void executes_and_commits_transaction_and_returns_result_asynchronously() throws SQLException {
        AtomicBoolean transactionStarted = new AtomicBoolean(false);
        AtomicBoolean continueTransaction = new AtomicBoolean(false);
        String transactionResult = "this is the result";

        CompletableFuture<String> futureResult = dataStore.executeWithResultAsync(connection -> {
            transactionStarted.set(true);
            while (!continueTransaction.get());
            return transactionResult;
        });

        while (!transactionStarted.get());
        assertThat(futureResult.isDone(), is(false));
        continueTransaction.set(true);

        assertThat(futureResult.join(), is(transactionResult));
        assertCommitRollbackAndCloseCalledInThisOrder();
    }

    @Test
    public void executes_and_commits_transaction_with_no_results_asynchronously() throws SQLException {
        AtomicBoolean transactionStarted = new AtomicBoolean(false);
        AtomicBoolean transactionEnded = new AtomicBoolean(false);
        AtomicBoolean continueTransaction = new AtomicBoolean(false);

        CompletableFuture<Void> futureResult = dataStore.executeAsync(connection -> {
            transactionStarted.set(true);
            while (!continueTransaction.get());
            transactionEnded.set(true);
        });

        while (!transactionStarted.get());
        assertThat(futureResult.isDone(), is(false));
        assertThat(transactionEnded.get(), is(false));
        continueTransaction.set(true);

        futureResult.join();
        assertThat(transactionEnded.get(), is(true));
        assertCommitRollbackAndCloseCalledInThisOrder();
    }

    @Test
    public void asynchronous_execution_rolls_back_transaction_correctly_when_exception_thrown()
            throws ExecutionException, InterruptedException, SQLException {

        assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalledWhenExceptionThrown(() -> {
            CompletableFuture<String> futureResult = dataStore.executeWithResultAsync(connection -> {
                throw new RuntimeException();
            });
            futureResult.get();
        });

        assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalledWhenExceptionThrown(() -> {
            CompletableFuture<Void> futureResult = dataStore.executeAsync(connection -> {
                throw new RuntimeException();
            });
            futureResult.get();
        });
    }

    private void assertCommitRollbackAndCloseCalledInThisOrder() throws SQLException {
        InOrder order = inOrder(jdbcConnection);
        order.verify(jdbcConnection, times(1)).commit();
        order.verify(jdbcConnection, times(1)).rollback();
        order.verify(jdbcConnection, times(1)).close();
    }

    private void assertRollbackAndCloseCalledInThisOrderAndCommitNeverCalledWhenExceptionThrown(VoidCodeBlock codeBlock) throws SQLException {
        try {
            codeBlock.execute();
            fail("Exception expected!");
        } catch (Exception ignored) {
            verify(jdbcConnection, times(0)).commit();
            InOrder order = inOrder(jdbcConnection);
            order.verify(jdbcConnection, times(1)).rollback();
            order.verify(jdbcConnection, times(1)).close();
        }
    }

    @FunctionalInterface
    public interface VoidCodeBlock {
        void execute() throws Exception;
    }
}
