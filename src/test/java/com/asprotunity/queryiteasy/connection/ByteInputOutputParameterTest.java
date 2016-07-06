package com.asprotunity.queryiteasy.connection;

import org.junit.Test;
import org.mockito.InOrder;

import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class ByteInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        Byte value = 10;
        ByteInputOutputParameter outputParameter = new ByteInputOutputParameter(value);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(outputParameter);

        assertThat(outputParameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).setObject(position, value, Types.TINYINT);
        order.verify(statement).registerOutParameter(position, Types.TINYINT);
        order.verify(statement).getObject(position);
    }

}