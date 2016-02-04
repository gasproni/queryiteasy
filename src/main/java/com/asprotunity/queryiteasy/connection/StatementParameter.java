package com.asprotunity.queryiteasy.connection;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@FunctionalInterface
public interface StatementParameter {

    void accept(StatementParameterBinder binder);

    static StatementParameter bind(String value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Short value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Integer value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Long value) {
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

    static StatementParameter bind(Boolean value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(BigDecimal value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Date value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Time value) {
        return binder -> binder.bind(value);
    }

    static StatementParameter bind(Timestamp value) {
        return binder -> binder.bind(value);
    }
}
