package com.asprotunity.jdbcunboil;


import com.asprotunity.jdbcunboil.connection.Connection;

@FunctionalInterface
public interface QueryTransaction<ResultType> {
    ResultType execute(Connection connection);
}
