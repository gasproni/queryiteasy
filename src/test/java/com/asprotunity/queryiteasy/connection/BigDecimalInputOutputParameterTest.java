package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class BigDecimalInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        BigDecimal value = new BigDecimal(10.3);
        BigDecimalInputOutputParameter outputParameter = new BigDecimalInputOutputParameter(value);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(outputParameter);

        assertThat(outputParameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).setBigDecimal(position, value);
        order.verify(statement).registerOutParameter(position, Types.DECIMAL);
        order.verify(statement).getObject(position);
    }

}