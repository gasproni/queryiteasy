package com.asprotunity.tersql.connection;


public interface Row {
    String asString(String columnName);
    Integer asInteger(String columnName);
    Double asDouble(String columnName);
    Float asFloat(String columnName);
    Byte asByte(String columnName);
}
