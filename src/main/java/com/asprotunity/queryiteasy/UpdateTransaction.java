package com.asprotunity.queryiteasy;


import com.asprotunity.queryiteasy.connection.Connection;

@FunctionalInterface
public interface UpdateTransaction {
    void execute(Connection connection);
}
