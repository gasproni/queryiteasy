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

public class BooleanInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        BooleanInputOutputParameter parameter = new BooleanInputOutputParameter(true);
        assertThat(parameter.value(), is(true));
    }


    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Boolean inputValue = true;
        Boolean outputValue = false;
        BooleanInputOutputParameter parameter = new BooleanInputOutputParameter(inputValue);
        when(statement.getBoolean(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.BOOLEAN);
        order.verify(statement).registerOutParameter(position, Types.BOOLEAN);
        order.verify(statement).getBoolean(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        BooleanInputOutputParameter parameter = new BooleanInputOutputParameter(null);
        when(statement.getBoolean(position)).thenReturn(false);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}