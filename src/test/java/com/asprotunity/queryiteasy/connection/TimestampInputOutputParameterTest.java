package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class TimestampInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Timestamp inputValue = new Timestamp(123456789L);
        Timestamp outputValue = new Timestamp(99999999L);
        TimestampInputOutputParameter parameter = new TimestampInputOutputParameter(inputValue);
        when(statement.getObject(position)).thenReturn(outputValue);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputValue));
        InOrder order = inOrder(statement);
        order.verify(statement).setTimestamp(position, inputValue);
        order.verify(statement).registerOutParameter(position, Types.TIMESTAMP);
        order.verify(statement).getObject(position);
    }

}