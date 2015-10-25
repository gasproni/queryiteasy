package com.asprotunity.queryiteasy.connection;


public interface Statement {
    void execute();
    void setString(int position, String value);
    void setInt(int position, int value);
    void setDouble(int position, double value);
}
