package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class BooleanInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        boolean inputValue = true;
        BooleanInputOutputParameter parameter = new BooleanInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }


    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Boolean inputValue = true;
        Boolean outputValue = false;
        BooleanInputOutputParameter parameter = new BooleanInputOutputParameter(inputValue);
        when(statement.getObject(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.BOOLEAN);
        order.verify(statement).registerOutParameter(position, Types.BOOLEAN);
        order.verify(statement).getObject(position);
    }

}