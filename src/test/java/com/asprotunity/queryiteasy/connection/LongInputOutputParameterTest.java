package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class LongInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        Long inputValue = 10L;
        LongInputOutputParameter parameter = new LongInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Long inputValue = 10L;
        Long outputValue = 10L;
        LongInputOutputParameter parameter = new LongInputOutputParameter(inputValue);
        when(statement.getLong(position)).thenReturn(outputValue);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.BIGINT);
        order.verify(statement).registerOutParameter(position, Types.BIGINT);
        order.verify(statement).getLong(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        LongInputOutputParameter parameter = new LongInputOutputParameter(0L);
        when(statement.getLong(position)).thenReturn(0L);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}