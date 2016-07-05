package com.asprotunity.queryiteasy.connection;

public abstract class InputOutputParameter<ValueType> implements OutputParameter {
    private ValueType value = null;

    public InputOutputParameter(ValueType value) {
        setValue(value);
    }

    public ValueType value() {
        return value;
    }

    protected void setValue(ValueType value) {
        this.value = value;
    }
}
