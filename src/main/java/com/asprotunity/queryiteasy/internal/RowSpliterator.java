package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.sql.ResultSet;
import java.util.Spliterator;
import java.util.function.Consumer;

public class RowSpliterator implements Spliterator<Row> {

    private final ResultSet resultSet;

    public RowSpliterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            if (resultSet.next()) {
                action.accept(new RowFromResultSet(resultSet));
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
        return 0;
    }

    @Override
    public int characteristics() {
        return IMMUTABLE | NONNULL | DISTINCT;
    }
}
