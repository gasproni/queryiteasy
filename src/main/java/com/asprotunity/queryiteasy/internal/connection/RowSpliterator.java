package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.util.Spliterator;
import java.util.function.Consumer;

public class RowSpliterator implements Spliterator<ResultSet> {

    private ResultSet resultSet;

    public RowSpliterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResultSet> action) {
        return RuntimeSQLException.executeWithResult(() -> {
            if (resultSet.next()) {
                action.accept(resultSet);
                return true;
            }
            return false;
        });
    }

    @Override
    public Spliterator<ResultSet> trySplit() {
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
