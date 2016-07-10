package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class ClobOutputParameterTest extends OutputParameterTestBase {

    @Test(expected = InvalidArgumentException.class)
    public void throws_when_clobreader_is_null() {
        new ClobOutputParameter<String>(null);
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {

        String blobContent = "this is the content of the clob";
        Function<Reader, String> clobReader = inputStream -> blobContent;
        ClobOutputParameter<String> parameter = new ClobOutputParameter<>(clobReader);

        Clob value = mock(Clob.class);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(blobContent));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.CLOB);
        order.verify(statement).getObject(position);
    }

}