package com.asprotunity.queryiteasy.connection;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@FunctionalInterface
public interface InputParameter {

    void accept(InputParameterBinder binder);

    static InputParameter bind(String value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Short value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Integer value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Long value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Double value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Float value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Byte value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Boolean value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(BigDecimal value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Date value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Time value) {
        return binder -> binder.bind(value);
    }

    static InputParameter bind(Timestamp value) {
        return binder -> binder.bind(value);
    }
}
