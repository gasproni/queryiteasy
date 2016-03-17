package com.asprotunity.queryiteasy.connection;

public abstract class InputOutputParameter<ValueType> extends OutputParameter<ValueType> {
    public InputOutputParameter(ValueType value) {
        setValue(value);
    }
}
