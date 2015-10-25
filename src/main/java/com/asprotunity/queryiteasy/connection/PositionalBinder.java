package com.asprotunity.queryiteasy.connection;

@FunctionalInterface
public interface PositionalBinder {

    void apply(Statement statement, int position);

    static PositionalBinder bind(String value) {
        return (statement, position) -> statement.setString(position, value);
    }

    static PositionalBinder bind(int value) {
        return (statement, position) -> statement.setInt(position, value);
    }

    static PositionalBinder bind(double value) {
        return (statement, position) -> statement.setDouble(position, value);
    }
}
