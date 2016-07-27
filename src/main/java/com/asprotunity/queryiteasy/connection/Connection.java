package com.asprotunity.queryiteasy.connection;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {
    void update(String sql, InputParameter... parameters);

    void update(String sql, List<Batch> batches);

    <MappedRowType> Stream<MappedRowType> select(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                                 InputParameter... parameters);

    void call(String sql, Parameter... parameters);

    <MappedRowType> Stream<MappedRowType> call(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                               Parameter... parameters);
}
