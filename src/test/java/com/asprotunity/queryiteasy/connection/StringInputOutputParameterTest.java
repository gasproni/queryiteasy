package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class StringInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        String inputValue = "input value";
        StringInputOutputParameter parameter = new StringInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        String inputValue = "input value";
        String outputValue = "output value";
        StringInputOutputParameter parameter = new StringInputOutputParameter(inputValue);
        when(statement.getString(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setString(position, inputValue);
        order.verify(statement).registerOutParameter(position, Types.LONGVARCHAR);
        order.verify(statement).getString(position);
    }

}