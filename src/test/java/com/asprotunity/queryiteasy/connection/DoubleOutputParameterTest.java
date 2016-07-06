package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class DoubleOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        DoubleOutputParameter outputParameter = new DoubleOutputParameter();
        Double value = 10.0;
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(outputParameter);

        assertThat(outputParameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.DOUBLE);
        order.verify(statement).getObject(position);
    }

}