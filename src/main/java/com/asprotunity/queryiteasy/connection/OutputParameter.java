package com.asprotunity.queryiteasy.connection;

public abstract class OutputParameter<ValueType> implements Parameter {
    private ValueType value = null;

    public ValueType value() {
        return value;
    }

    protected void setValue(ValueType value) {
        this.value = value;
    }
}
