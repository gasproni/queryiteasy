package com.asprotunity.jdbcunboil.connection;

@FunctionalInterface
public interface PositionalParameterFunction {
    void apply(StatementParameter parameter, int position);
}
