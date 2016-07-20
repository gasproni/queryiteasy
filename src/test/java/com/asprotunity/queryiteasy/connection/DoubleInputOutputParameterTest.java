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

public class DoubleInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        Double inputValue = 10.0;
        DoubleInputOutputParameter parameter = new DoubleInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Double inputValue = 10.0;
        Double outputValue = 15.0;
        DoubleInputOutputParameter parameter = new DoubleInputOutputParameter(inputValue);
        when(statement.getDouble(position)).thenReturn(outputValue);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.DOUBLE);
        order.verify(statement).registerOutParameter(position, Types.DOUBLE);
        order.verify(statement).getDouble(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        DoubleInputOutputParameter parameter = new DoubleInputOutputParameter(10d);
        when(statement.getDouble(position)).thenReturn(0d);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}