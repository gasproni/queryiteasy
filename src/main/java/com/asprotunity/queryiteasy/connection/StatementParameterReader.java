package com.asprotunity.queryiteasy.connection;

public interface StatementParameterReader {
    void setString(String value);

    void setInt(int value);

    void setDouble(double value);
}
