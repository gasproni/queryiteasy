package com.asprotunity.queryiteasy.connection;



@FunctionalInterface
public interface RowMapper<ResultType> {
    ResultType map(Row row);
}
