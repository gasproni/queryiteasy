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

public class ByteInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_constructor_parameter() {
        Byte inputValue = 10;
        ByteInputOutputParameter parameter = new ByteInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Byte inputValue = 10;
        Byte outputValue = 12;
        ByteInputOutputParameter parameter = new ByteInputOutputParameter(inputValue);
        when(statement.getByte(position)).thenReturn(outputValue);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, inputValue, Types.TINYINT);
        order.verify(statement).registerOutParameter(position, Types.TINYINT);
        order.verify(statement).getByte(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        ByteInputOutputParameter parameter = new ByteInputOutputParameter((byte)10);
        when(statement.getByte(position)).thenReturn((byte)0);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}