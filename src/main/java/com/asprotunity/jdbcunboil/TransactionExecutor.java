package com.asprotunity.jdbcunboil;

import com.asprotunity.jdbcunboil.connection.Connection;
import com.asprotunity.jdbcunboil.exception.InvalidArgumentException;
import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;
import com.asprotunity.jdbcunboil.internal.WrappedJDBCConnection;

import javax.sql.DataSource;
import java.util.function.Consumer;
import java.util.function.Function;

public class TransactionExecutor {

    private DataSource dataSource;

    public TransactionExecutor(DataSource dataSource) {
        if (dataSource == null) {
            throw new InvalidArgumentException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
    }

    public void execute(Consumer<Connection> transaction) {
        RuntimeSQLException.wrapException(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection())) {
                        transaction.accept(connection);
                        connection.commit();
                    }
                }
        );
    }


    public <ResultType> ResultType executeWithResult(Function<Connection, ResultType> transaction) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection())) {
                        ResultType result = transaction.apply(connection);
                        connection.commit();
                        return result;
                    }
                }
        );
    }
}
