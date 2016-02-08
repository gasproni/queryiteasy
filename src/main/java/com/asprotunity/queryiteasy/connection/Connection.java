package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;

import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {
    void update(String sql, StatementParameter... parameters);

    void updateBatch(String sql, Batch... batches);

    /**
     * @throws InvalidArgumentException, if batches array is empty.
     */
    void updateNonemptyBatch(String sql, Batch... batches);

    <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> rowProcessor,
                                   StatementParameter... parameters);
}
