package com.asprotunity.jdbcunboiled.connection;

@FunctionalInterface
public interface StatementParameter {

    void accept(StatementParameterBinder binder);

    static StatementParameter bind(String value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Integer value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Double value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Float value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Byte value) {
        return binder -> binder.bind(value);
    }
}
