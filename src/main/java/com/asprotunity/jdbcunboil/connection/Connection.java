package com.asprotunity.jdbcunboil.connection;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Connection {
    void update(String sql, StatementParameter... binders);

    void update(String sql, Batch firstBatch, Batch... batches);

    void select(String sql, Consumer<Row> rowConsumer, StatementParameter... parameters);
}
