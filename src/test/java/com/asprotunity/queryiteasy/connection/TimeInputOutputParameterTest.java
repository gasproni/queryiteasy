package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class TimeInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Time inputValue = new Time(123456789L);
        Time outputValue = new Time(99999999L);
        TimeInputOutputParameter parameter = new TimeInputOutputParameter(inputValue);
        when(statement.getObject(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setTime(position, inputValue);
        order.verify(statement).registerOutParameter(position, Types.TIME);
        order.verify(statement).getObject(position);
    }

}