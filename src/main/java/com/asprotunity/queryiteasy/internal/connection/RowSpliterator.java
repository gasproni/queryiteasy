package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RowFactory;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.util.Spliterator;
import java.util.function.Consumer;

public class RowSpliterator<RowType extends Row> implements Spliterator<RowType> {

    private ResultSet resultSet;
    private RowFactory<RowType> rowFactory;

    public RowSpliterator(ResultSet resultSet, RowFactory<RowType> rowFactory) {
        this.resultSet = resultSet;
        this.rowFactory = rowFactory;
    }

    @Override
    public boolean tryAdvance(Consumer<? super RowType> action) {
        return RuntimeSQLException.executeWithResult(() -> {
            if (resultSet.next()) {
                action.accept(rowFactory.make(resultSet));
                return true;
            }
            return false;
        });
    }

    @Override
    public Spliterator<RowType> trySplit() {
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
