package com.asprotunity.queryiteasy;

import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.internal.WrappedJDBCConnection;
import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import javax.sql.DataSource;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataStore {

    private DataSource dataSource;

    public DataStore(DataSource dataSource) {
        InvalidArgumentException.throwIfNull(dataSource, "dataSource");
        this.dataSource = dataSource;
    }

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
}
