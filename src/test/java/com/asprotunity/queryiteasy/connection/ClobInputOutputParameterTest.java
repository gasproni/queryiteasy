package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class ClobInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_null() {
        Reader inputClobReader = mock(Reader.class);
        ClobInputOutputParameter<String> parameter = new ClobInputOutputParameter<>(() -> inputClobReader,
                outputClobReader -> "doesn't matter");

        assertThat(parameter.value(), is(nullValue()));
        verifyZeroInteractions(inputClobReader);
    }

    @Test
    public void throws_when_outputClobReader_is_null() {
        try {
            new ClobInputOutputParameter<String>(() -> null, null);
            fail("InvalidArgumentException expected");
        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("outputClobReader cannot be null"));
        }
    }

    @Test
    public void throws_when_inputClobSupplier_is_null() {
        try {
            new ClobInputOutputParameter<String>(null, InputReader -> null);
            fail("InvalidArgumentException expected");

        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("inputClobSupplier cannot be null"));
        }
    }

    @Test
    public void binds_results_correctly_when_input_clob_not_null_and_statement_leaves_scope() throws SQLException, IOException {

        String outputClobContent = "this is the content of the clob";
        Reader inputClobReader = mock(Reader.class);
        Supplier<Reader> inputClobSupplier = () -> inputClobReader;
        Function<Reader, String> outputClobReader = inputReader -> outputClobContent;
        ClobInputOutputParameter<String> parameter = new ClobInputOutputParameter<>(inputClobSupplier, outputClobReader);

        Clob value = mock(Clob.class);
        when(statement.getClob(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputClobContent));
        InOrder order = inOrder(statement, inputClobReader);
        order.verify(statement).setClob(position, inputClobReader);
        order.verify(statement).registerOutParameter(position, Types.CLOB);
        order.verify(statement).getClob(position);
        order.verify(inputClobReader).close();
    }

    @Test
    public void binds_results_correctly_when_input_clob_is_null_and_statement_leaves_scope() throws SQLException, IOException {

        String clobContent = "this is the content of the clob";
        Supplier<Reader> inputClobSupplier = () -> null;
        Function<Reader, String> outputClobReader = inputReader -> clobContent;
        ClobInputOutputParameter<String> parameter = new ClobInputOutputParameter<>(inputClobSupplier, outputClobReader);

        Clob value = mock(Clob.class);
        when(statement.getClob(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(clobContent));
        InOrder order = inOrder(statement);
        order.verify(statement).setNull(position, Types.CLOB);
        order.verify(statement).registerOutParameter(position, Types.CLOB);
        order.verify(statement).getClob(position);
    }

}