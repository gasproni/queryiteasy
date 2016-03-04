package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.functional.ThrowingFunction;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {
    void update(String sql, InputParameter... parameters);

    void update(String sql, List<Batch> batches);

    <ResultType> ResultType select(String sql, ThrowingFunction<Stream<Row>, ResultType> rowProcessor,
                                   InputParameter... parameters);
}
