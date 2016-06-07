package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.closer.Closer;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.util.Spliterator;
import java.util.function.Consumer;

public class RowSpliterator implements Spliterator<Row> {

    private ResultSetWrapper rs;
    private Closer connectionCloser;

    public RowSpliterator(ResultSetWrapper resultSetWrapper, Closer connectionCloser) {
        this.rs = resultSetWrapper;
        this.connectionCloser = connectionCloser;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            if (rs.next()) {
                action.accept(new RowFromResultSet(rs, connectionCloser));
                return true;
            }
            return false;
        });
    }

    @Override
    public Spliterator<Row> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return IMMUTABLE | NONNULL | DISTINCT;
    }
}
