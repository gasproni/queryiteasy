package com.asprotunity.queryiteasy;


@FunctionalInterface
public interface QueryTransaction<ResultType> {
    ResultType execute(Connection connection);
}
