package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class DateInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Date inputValue = new Date(123456789L);
        Date outputValue = new Date(99999999L);
        DateInputOutputParameter parameter = new DateInputOutputParameter(inputValue);
        when(statement.getObject(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setDate(position, inputValue);
        order.verify(statement).registerOutParameter(position, Types.DATE);
        order.verify(statement).getObject(position);
    }

}