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

public class DateOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {
        DateOutputParameter outputParameter = new DateOutputParameter();
        long doesntMatter = 123456789L;
        Date value = new Date(doesntMatter);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndMakeCall(outputParameter);

        assertThat(outputParameter.value(), is(value));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.DATE);
        order.verify(statement).getObject(position);
    }

}