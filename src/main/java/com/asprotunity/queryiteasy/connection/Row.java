package com.asprotunity.queryiteasy.connection;


public interface Row {

    Object at(String columnLabel);

    Object at(int columnPosition);

    int columnCount();

}
