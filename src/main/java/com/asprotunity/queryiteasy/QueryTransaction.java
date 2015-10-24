package com.asprotunity.queryiteasy;


import com.asprotunity.queryiteasy.connection.Connection;

@FunctionalInterface
public interface QueryTransaction<ResultType> {
    ResultType execute(Connection connection);
}
