package com.asprotunity.queryiteasy.connection;


import java.io.InputStream;

public interface Row {

    Object at(String columnLabel);

    Object at(int columnPosition);

    InputStream binaryStreamAt(String columnLabel);

    InputStream binaryStreamAt(int columnPosition);

    int columnCount();

}
