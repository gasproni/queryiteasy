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

public class ShortInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        Short inputValue = 10;
        ShortInputOutputParameter parameter = new ShortInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Short inputValue = 10;
        Short outputValue = 10;
        ShortInputOutputParameter parameter = new ShortInputOutputParameter(inputValue);
        when(statement.getShort(position)).thenReturn(outputValue);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.SMALLINT);
        order.verify(statement).registerOutParameter(position, Types.SMALLINT);
        order.verify(statement).getShort(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        ShortInputOutputParameter parameter = new ShortInputOutputParameter((short)0);
        when(statement.getShort(position)).thenReturn((short)0);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }
}