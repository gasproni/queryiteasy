package com.asprotunity.queryiteasy.connection;

@FunctionalInterface
public interface PositionalParameterFunction {
    void apply(StatementParameter parameter, int position);
}
