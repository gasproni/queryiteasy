package com.asprotunity.jdbcunboil.connection;

@FunctionalInterface
public interface StatementParameter {

    void apply(StatementParameterFunction action);

    static StatementParameter bind(String value) {
        return action -> action.applyTo(value);
    }

    static StatementParameter bind(Integer value) {
        return action -> action.applyTo(value);
    }

    static StatementParameter bind(Double value) {
        return action -> action.applyTo(value);
    }
}
