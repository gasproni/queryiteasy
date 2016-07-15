package com.asprotunity.queryiteasy.connection;


import java.io.InputStream;
import java.io.Reader;

public interface Row {

    Object at(String columnLabel);

    Object at(int columnPosition);

    InputStream binaryStreamAt(String columnLabel);

    InputStream binaryStreamAt(int columnPosition);

    InputStream asciiStreamAt(String columnLabel);

    InputStream asciiStreamAt(int columnPosition);

    Reader characterStreamAt(String columnLabel);

    Reader characterStreamAt(int columnPosition);

    Reader nCharacterStreamAt(String columnLabel);

    Reader nCharacterStreamAt(int columnPosition);

    int columnCount();

}
