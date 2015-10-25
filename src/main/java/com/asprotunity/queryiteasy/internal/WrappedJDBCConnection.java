package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.PositionalBinder;
import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.connection.Statement;

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
    public void executeUpdate(String sql, PositionalBinder...binders) {
        RuntimeSQLException.wrapException(() ->
                {
                    Statement statement = new WrappedPreparedStatement(connection.prepareStatement(sql));
                    for (int index = 0; index < binders.length; ++index) {
                        binders[index].apply(statement, index + 1);
                    }
                    statement.execute();
                }
        );
    }
}
