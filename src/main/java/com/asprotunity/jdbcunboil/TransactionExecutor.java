package com.asprotunity.jdbcunboil;

import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;
import com.asprotunity.jdbcunboil.internal.WrappedJDBCConnection;

import javax.sql.DataSource;

public class TransactionExecutor {

    private DataSource dataSource;

    public TransactionExecutor(DataSource dataSource) {
        if (dataSource == null) {
            throw new NullPointerException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
    }

    public void executeUpdate(UpdateTransaction transaction) {
        RuntimeSQLException.wrapException(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection())) {
                        transaction.execute(connection);
                        connection.commit();
                    }
                }
        );
    }


    public <ResultType> ResultType executeQuery(QueryTransaction<ResultType> transaction) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection())) {
                        ResultType result = transaction.execute(connection);
                        connection.commit();
                        return result;
                    }
                }
        );
    }
}
