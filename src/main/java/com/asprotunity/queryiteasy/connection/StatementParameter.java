package com.asprotunity.queryiteasy.connection;

@FunctionalInterface
public interface StatementParameter {

    void apply(StatementParameterAction action);

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
