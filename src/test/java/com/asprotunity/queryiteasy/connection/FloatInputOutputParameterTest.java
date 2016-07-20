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

public class FloatInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        Float inputValue = 10F;
        FloatInputOutputParameter parameter = new FloatInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Float inputValue = 10F;
        Float outputValue = 15F;
        FloatInputOutputParameter parameter = new FloatInputOutputParameter(inputValue);
        when(statement.getFloat(position)).thenReturn(outputValue);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.REAL);
        order.verify(statement).registerOutParameter(position, Types.REAL);
        order.verify(statement).getFloat(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        FloatInputOutputParameter parameter = new FloatInputOutputParameter(0f);
        when(statement.getFloat(position)).thenReturn(0f);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}