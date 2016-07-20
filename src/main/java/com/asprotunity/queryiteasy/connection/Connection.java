package com.asprotunity.queryiteasy.connection;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection<RowType> {
    void update(String sql, InputParameter... parameters);

    void update(String sql, List<Batch> batches);

    <MappedRowType> Stream<MappedRowType> select(Function<RowType, MappedRowType> rowMapper, String sql,
                                                 InputParameter... parameters);

    void call(String sql, Parameter... parameters);

    <MappedRowType> Stream<MappedRowType> call(Function<RowType, MappedRowType> rowMapper, String sql,
                                               Parameter... parameters);
}
