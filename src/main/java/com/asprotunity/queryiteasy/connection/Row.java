package com.asprotunity.queryiteasy.connection;


public interface Row {
    Integer getInteger(String columnName);
    String getString(String columnName);
}
