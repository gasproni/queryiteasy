package com.asprotunity.jdbcunboil.userprovided;


import com.asprotunity.jdbcunboil.row.Row;

@FunctionalInterface
public interface RowMapper<ResultType> {
    ResultType map(Row row);
}
