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

public class IntegerOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        IntegerOutputParameter parameter = new IntegerOutputParameter();
        Integer value = 10;
        when(statement.getInt(position)).thenReturn(value);
        when(statement.wasNull()).thenReturn(false);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.INTEGER);
        order.verify(statement).getInt(position);
    }

    @Test
    public void binds_result_correctly_when_result_null() throws SQLException {
        IntegerOutputParameter parameter = new IntegerOutputParameter();
        when(statement.getInt(position)).thenReturn(0);
        when(statement.wasNull()).thenReturn(true);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(nullValue()));
    }

}