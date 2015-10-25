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
    public void executeUpdate(String sql, PositionalBinder... binders) {
        Statement statement = new WrappedPreparedStatement(connection, sql);
        for (int index = 0; index < binders.length; ++index) {
            binders[index].apply(statement, index + 1);
        }
        statement.execute();
    }

    @Override
    public <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper, PositionalBinder... binders) {
        Statement statement = new WrappedPreparedStatement(connection, sql);
        for (int index = 0; index < binders.length; ++index) {
            binders[index].apply(statement, index + 1);
        }
        return statement.executeQuery(rowMapper);
    }
}
