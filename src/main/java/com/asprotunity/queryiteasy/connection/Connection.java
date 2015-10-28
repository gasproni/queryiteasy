package com.asprotunity.queryiteasy.connection;

import java.util.List;

public interface Connection {
    void executeUpdate(String sql, StatementParameter...binders);
    void executeBatchUpdate(String sql, Batch firstBatch, Batch...batches);
    <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper, StatementParameter... binders);
}
