package com.asprotunity.queryiteasy.connection;


public interface Row {
    int getInt(String columnName);
    String getString(String columnName);
}
