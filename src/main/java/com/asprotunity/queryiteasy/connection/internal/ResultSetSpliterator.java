package com.asprotunity.queryiteasy.connection.internal;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ResultSetSpliterator implements Spliterator<ResultSet> {

    private ResultSet resultSet;

    public ResultSetSpliterator(ResultSet resultSet) {
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
