package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.GenericRow;
import com.asprotunity.queryiteasy.connection.RowFactory;
import org.junit.Test;
import org.mockito.InOrder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

import static java.util.Spliterator.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RowSpliteratorTest {

    private final ResultSet resultSet = mock(ResultSet.class);
    @SuppressWarnings("unchecked")
    private final RowFactory<GenericRow> factory = mock(RowFactory.class);
    private final RowSpliterator spliterator = new RowSpliterator<>(resultSet, factory);

    @Test
    @SuppressWarnings("unchecked")
    public void creates_row_when_result_set_has_data() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        Consumer<GenericRow> consumer = mock(Consumer.class);

        boolean result = spliterator.tryAdvance(consumer);

        assertTrue(result);
        InOrder order = inOrder(resultSet, factory);
        order.verify(resultSet, times(1)).next();
        order.verify(factory, times(1)).make(resultSet);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void no_row_created_when_result_set_has_no_data() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        Consumer<GenericRow> consumer = mock(Consumer.class);

        boolean result = spliterator.tryAdvance(consumer);

        assertFalse(result);
        verify(resultSet, times(1)).next();
        verify(factory, times(0)).make(any());
    }

    @Test
    public void try_split_returns_null() {
        assertThat(spliterator.trySplit(), is(nullValue()));
    }

    @Test
    public void estimate_size_returns_long_max_value() {
        assertThat(spliterator.estimateSize(), is(Long.MAX_VALUE));
    }

    @Test
    public void characteristics_are_immutable_nonnull_distinct() {
        assertThat(spliterator.characteristics(), is(IMMUTABLE | NONNULL | DISTINCT));
    }

}