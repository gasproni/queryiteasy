package com.asprotunity.queryiteasy;

import javax.sql.DataSource;

public class TransactionExecutor {

    private DataSource dataSource;

    public TransactionExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void executeUpdate(UpdateTransaction transaction) {
        SQLException.wrapException(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection())) {
                        transaction.execute(connection);
                        connection.commit();
                    }
                }
        );
    }


    public <ResultType> ResultType executeQuery(QueryTransaction<ResultType> transaction) {
        return SQLException.wrapExceptionAndReturnResult(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection())) {
                        ResultType result = transaction.execute(connection);
                        connection.commit();
                        return result;
                    }
                }
        );
    }
}
