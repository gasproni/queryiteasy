package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.util.Spliterator;
import java.util.function.Consumer;

public class RowSpliterator implements Spliterator<Row> {

    private ResultSetWrapper rs;
    private Scope connectionScope;

    public RowSpliterator(ResultSetWrapper resultSetWrapper, Scope connectionScope) {
        this.rs = resultSetWrapper;
        this.connectionScope = connectionScope;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            if (rs.next()) {
                action.accept(new RowFromResultSet(rs, connectionScope));
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
