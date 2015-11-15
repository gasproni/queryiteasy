package com.asprotunity.jdbcunboil.connection;

import com.asprotunity.jdbcunboil.userprovided.RowMapper;

import java.util.List;

public interface Connection {
    void executeUpdate(String sql, StatementParameter...binders);
    void executeUpdate(String sql, Batch firstBatch, Batch...batches);
    <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper,
                                               StatementParameter... parameters);
}
