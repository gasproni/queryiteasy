package com.asprotunity.jdbcunboiled.connection;

import java.util.function.Consumer;

public interface Connection {
    void update(String sql, StatementParameter... parameters);

    void update(String sql, Batch firstBatch, Batch... batches);

    void select(String sql, Consumer<Row> rowConsumer, StatementParameter... parameters);
}
