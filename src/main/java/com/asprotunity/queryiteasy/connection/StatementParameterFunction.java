package com.asprotunity.queryiteasy.connection;

public interface StatementParameterFunction {
    void applyTo(String value);

    void applyTo(Integer value);

    void applyTo(Double value);
}
