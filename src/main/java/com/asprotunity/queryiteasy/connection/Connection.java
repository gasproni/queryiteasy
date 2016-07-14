package com.asprotunity.queryiteasy.connection;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {
    void update(String sql, InputParameter... parameters);

    void update(String sql, List<Batch> batches);

    <MappedRow> Stream<MappedRow> select(Function<Row, MappedRow> rowMapper, String sql,
                                         InputParameter... parameters);

    void call(String sql, Parameter... parameters);

    <ResultType> ResultType call(Function<Stream<Row>, ResultType> rowProcessor, String sql,
                                 Parameter... parameters);
}
