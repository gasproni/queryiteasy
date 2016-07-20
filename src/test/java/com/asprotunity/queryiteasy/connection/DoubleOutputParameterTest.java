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

public class DoubleOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        DoubleOutputParameter parameter = new DoubleOutputParameter();
        Double value = 10.0;
        when(statement.getDouble(position)).thenReturn(value);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.DOUBLE);
        order.verify(statement).getDouble(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        DoubleOutputParameter parameter = new DoubleOutputParameter();
        when(statement.getDouble(position)).thenReturn(0d);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}