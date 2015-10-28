package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.*;

import java.util.List;

public class WrappedJDBCConnection implements Connection, AutoCloseable {
    private java.sql.Connection connection;

    public WrappedJDBCConnection(java.sql.Connection connection) {
        this.connection = connection;
        RuntimeSQLException.wrapException(() -> this.connection.setAutoCommit(false));
    }

    public void commit() {
        RuntimeSQLException.wrapException(connection::commit);
    }

    @Override
    public void close() {
        RuntimeSQLException.wrapException(() -> {
            connection.rollback();
            connection.close();
        });
    }

    @Override
    public void executeUpdate(String sql, StatementParameter... parameters) {
        WrappedPreparedStatement statement = new WrappedPreparedStatement(connection, sql);
        statement.execute(parameters);
    }

    @Override
    public void executeBatchUpdate(String sql, Batch firstBatch, Batch... batches) {
        WrappedPreparedStatement statement = new WrappedPreparedStatement(connection, sql);
        statement.executeBatch(firstBatch, batches);
    }

    @Override
    public <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper, StatementParameter... parameters) {
        WrappedPreparedStatement statement = new WrappedPreparedStatement(connection, sql);
        return statement.executeQuery(rowMapper, parameters);
    }
}
