package com.asprotunity.queryiteasy.userprovided;


import com.asprotunity.queryiteasy.row.Row;

@FunctionalInterface
public interface RowMapper<ResultType> {
    ResultType map(Row row);
}
