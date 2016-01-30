package com.asprotunity.jdbcunboiled.internal;

import com.asprotunity.jdbcunboiled.connection.Row;
import com.asprotunity.jdbcunboiled.exception.RuntimeSQLException;

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
                action.accept(new WrappedResultSet(resultSet));
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
