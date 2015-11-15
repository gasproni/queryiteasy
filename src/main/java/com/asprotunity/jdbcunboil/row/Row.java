package com.asprotunity.jdbcunboil.row;


public interface Row {
    Integer getInteger(String columnName);
    String getString(String columnName);
}
