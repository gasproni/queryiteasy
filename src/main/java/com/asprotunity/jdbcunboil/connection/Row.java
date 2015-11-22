package com.asprotunity.jdbcunboil.connection;


public interface Row {
    Integer getInteger(String columnName);
    String getString(String columnName);
    Double getDouble(String columnName);
}
