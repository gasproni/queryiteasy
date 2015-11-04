package com.asprotunity.queryiteasy.connection;

public interface StatementParameterReader {
    void setString(String value);

    void setInteger(Integer value);

    void setDouble(Double value);
}
