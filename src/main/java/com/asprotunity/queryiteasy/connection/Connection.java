package com.asprotunity.queryiteasy.connection;

import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {
    void update(String sql, StatementParameter... parameters);

    void updateBatch(String sql, Batch... batches);

    <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> rowProcessor,
                                   StatementParameter... parameters);
}
