package com.asprotunity.queryiteasy.connection;

public interface Connection {
    void executeUpdate(String sql, PositionalBinder...binders);
}
