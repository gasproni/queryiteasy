package com.asprotunity.queryiteasy;

public class WrappedJDBCConnection implements CommittableConnection {
    private java.sql.Connection connection;

    public WrappedJDBCConnection(java.sql.Connection connection) {
        this.connection = connection;
        SQLException.wrapException(() -> this.connection.setAutoCommit(false));
    }

    @Override
    public void commit() {
        SQLException.wrapException(connection::commit);
    }

    @Override
    public void close() {
        SQLException.wrapException(() -> {
            connection.rollback();
            connection.close();
        });
    }
}
