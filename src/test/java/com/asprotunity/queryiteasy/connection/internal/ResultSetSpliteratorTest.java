package com.asprotunity.queryiteasy.connection.internal;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

import static java.util.Spliterator.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ResultSetSpliteratorTest {

    private final ResultSet resultSet = mock(ResultSet.class);
    private final ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet);

    @Test
    @SuppressWarnings("unchecked")
    public void creates_row_when_result_set_has_data() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        Consumer<ResultSet> consumer = mock(Consumer.class);

        boolean result = spliterator.tryAdvance(consumer);

        assertTrue(result);
        verify(resultSet, times(1)).next();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void no_row_created_when_result_set_has_no_data() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        Consumer<ResultSet> consumer = mock(Consumer.class);

        boolean result = spliterator.tryAdvance(consumer);

        assertFalse(result);
        verify(resultSet, times(1)).next();
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