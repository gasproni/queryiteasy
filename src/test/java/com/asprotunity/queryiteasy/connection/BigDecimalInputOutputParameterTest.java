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
    public void value_is_initialized_with_constructor_parameter() {
        BigDecimal inputValue = new BigDecimal(10.3);
        BigDecimalInputOutputParameter parameter = new BigDecimalInputOutputParameter(inputValue);
        assertThat(parameter.value(), is(inputValue));
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        BigDecimal inputValue = new BigDecimal(10.3);
        BigDecimal outputValue = new BigDecimal(12.5);
        BigDecimalInputOutputParameter parameter = new BigDecimalInputOutputParameter(inputValue);
        when(statement.getBigDecimal(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setBigDecimal(position, inputValue);
        order.verify(statement).registerOutParameter(position, Types.DECIMAL);
        order.verify(statement).getBigDecimal(position);
    }

}