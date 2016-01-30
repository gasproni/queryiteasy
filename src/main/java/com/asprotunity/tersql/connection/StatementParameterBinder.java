package com.asprotunity.tersql.connection;

public interface StatementParameterBinder {
    void bind(String value);

    void bind(Integer value);

    void bind(Double value);

    void bind(Float value);

    void bind(Byte value);
}
