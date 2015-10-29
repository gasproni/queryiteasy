package com.asprotunity.queryiteasy.connection;

@FunctionalInterface
public interface StatementParameter {

    void readValue(StatementParameterReader valueReader);

    static StatementParameter bind(String value) {
        return valueReader -> valueReader.setString(value);
    }

    static StatementParameter bind(Integer value) {
        return valueReader -> valueReader.setInteger(value);
    }

    static StatementParameter bind(double value) {
        return valueReader -> valueReader.setDouble(value);
    }
}
