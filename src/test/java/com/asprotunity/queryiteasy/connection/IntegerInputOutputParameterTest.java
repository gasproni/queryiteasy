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

public class IntegerInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        Integer inputValue = 10;
        IntegerInputOutputParameter parameter = new IntegerInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Integer inputValue = 10;
        Integer outputValue = 10;
        IntegerInputOutputParameter parameter = new IntegerInputOutputParameter(inputValue);
        when(statement.getInt(position)).thenReturn(outputValue);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.INTEGER);
        order.verify(statement).registerOutParameter(position, Types.INTEGER);
        order.verify(statement).getInt(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        IntegerInputOutputParameter parameter = new IntegerInputOutputParameter(0);
        when(statement.getInt(position)).thenReturn(0);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}