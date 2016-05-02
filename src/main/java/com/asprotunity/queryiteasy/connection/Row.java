package com.asprotunity.queryiteasy.connection;


public interface Row {

    Object at(String columnName);

    Object at(int columnPosition);

}
