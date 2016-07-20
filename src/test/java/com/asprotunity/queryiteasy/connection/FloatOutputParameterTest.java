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

public class FloatOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_result_correctly_when_statement_leaves_scope() throws SQLException {
        FloatOutputParameter parameter = new FloatOutputParameter();
        Float value = 10F;
        when(statement.getFloat(position)).thenReturn(value);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.REAL);
        order.verify(statement).getFloat(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        FloatOutputParameter parameter = new FloatOutputParameter();
        when(statement.getFloat(position)).thenReturn(0f);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}