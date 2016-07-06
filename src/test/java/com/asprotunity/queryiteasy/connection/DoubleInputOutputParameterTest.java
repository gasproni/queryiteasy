package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class DoubleInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Double value = 10.0;
        DoubleInputOutputParameter parameter = new DoubleInputOutputParameter(value);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, value, Types.DOUBLE);
        order.verify(statement).registerOutParameter(position, Types.DOUBLE);
        order.verify(statement).getObject(position);
    }

}