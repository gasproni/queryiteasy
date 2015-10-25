package com.asprotunity.queryiteasy.connection;

import java.util.List;

public interface Connection {
    void executeUpdate(String sql, PositionalBinder...binders);
    void executeBatchUpdate(String sql, Batch...batches);

    <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper, PositionalBinder... binders);
}
