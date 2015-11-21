package com.asprotunity.jdbcunboil.connection;

import java.util.List;
import java.util.function.Function;

public interface Connection {
    void executeUpdate(String sql, StatementParameter... binders);

    void executeUpdate(String sql, Batch firstBatch, Batch... batches);

    <ResultType> List<ResultType> executeQuery(String sql, Function<Row, ResultType> rowMapper,
                                               StatementParameter... parameters);
}
