package com.asprotunity.queryiteasy.datastore;

import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.internal.WrappedJDBCConnection;
import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataStore {

    private DataSource dataSource;

    /**
     * Creates a DataStore instance that wraps the given JDBC DataSource.
     *
     * @param dataSource The JDBC DataSource to wrap.
     * @throws InvalidArgumentException if {@code dataSource == null}.
     */
    public DataStore(DataSource dataSource) {
        InvalidArgumentException.throwIfNull(dataSource, "dataSource");
        this.dataSource = dataSource;
    }

    /**
     * Executes the code inside the {@code transaction} lambda in a database transaction. If there are no exceptions the
     * transaction is committed and closed, otherwise it is rolled back and closed, and the exception re-thrown.
     *
     * @param transaction The transaction to execute.
     * @throws InvalidArgumentException if {@code transaction == null}.
     * @throws RuntimeSQLException      If a {@link java.sql.SQLException} is thrown during the call.
     */
    public void execute(Consumer<Connection> transaction) {
        InvalidArgumentException.throwIfNull(transaction, "transaction");
        RuntimeSQLException.execute(() -> {
                                        try (WrappedJDBCConnection connection =
                                                     new WrappedJDBCConnection(dataSource.getConnection())) {
                                            transaction.accept(connection);
                                            connection.commit();
                                        }
                                    }
        );
    }

    /**
     * Executes the transaction and returns {@code the result of transaction.apply(connection)}.
     * If there are no exceptions the transaction is committed and closed, otherwise it is rolled back and closed,
     * and the exception re-thrown.
     *
     * @param transaction  The transaction to execute.
     * @param <ResultType> A type provided by the caller.
     * @return The result of the transaction.
     * @throws InvalidArgumentException if {@code transaction == null}.
     * @throws RuntimeSQLException      If a {@link java.sql.SQLException} is thrown during the call.
     */
    public <ResultType> ResultType executeWithResult(Function<Connection, ResultType> transaction) {
        InvalidArgumentException.throwIfNull(transaction, "transaction");
        return RuntimeSQLException.executeWithResult(() -> {
                                                         try (WrappedJDBCConnection connection =
                                                                      new WrappedJDBCConnection(dataSource.getConnection())) {
                                                             ResultType result = transaction.apply(connection);
                                                             connection.commit();
                                                             return result;
                                                         }
                                                     }
        );
    }

    public <ResultType> CompletableFuture<ResultType> executeWithResultAsync(Function<Connection, ResultType> transaction) {
        return CompletableFuture.supplyAsync(() -> executeWithResult(transaction));
    }

    public CompletableFuture<Void> executeAsync(Consumer<Connection> transaction) {
        return CompletableFuture.supplyAsync(() -> {
            execute(transaction);
            return null;
        });
    }
}
