package com.asprotunity.queryiteasy.connection;



@FunctionalInterface
public interface RowMapper<ResultType> {
    ResultType apply(TableRow row);
}
