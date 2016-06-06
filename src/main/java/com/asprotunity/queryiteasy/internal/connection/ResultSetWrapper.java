package com.asprotunity.queryiteasy.internal.connection;

public interface ResultSetWrapper {
    boolean next();

    int columnCount();

    Object getObject(int columnIndex);

    String columnLabel(int columnIndex);
}
