package com.asprotunity.queryiteasy.connection;

@FunctionalInterface
public interface StatementParameter {

    void readValue(StatementParameterReader valueReader);

    static StatementParameter bind(String value) {
        return valueReader -> valueReader.setString(value);
    }

    static StatementParameter bind(int value) {
        return valueReader -> valueReader.setInt(value);
    }

    static StatementParameter bind(double value) {
        return valueReader -> valueReader.setDouble(value);
    }
}
