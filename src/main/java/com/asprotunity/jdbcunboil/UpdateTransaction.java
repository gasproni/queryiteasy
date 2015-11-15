package com.asprotunity.jdbcunboil;


import com.asprotunity.jdbcunboil.connection.Connection;

@FunctionalInterface
public interface UpdateTransaction {
    void execute(Connection connection);
}
