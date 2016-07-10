package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.stringio.StringIO.readFrom;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class LongVarBinaryInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_null() {
        InputStream inputLongVarBinaryStream = mock(InputStream.class);
        LongVarBinaryInputOutputParameter<String> parameter =
                new LongVarBinaryInputOutputParameter<>(() -> inputLongVarBinaryStream,
                        outputLongVarBinaryStream -> "doesn't matter");

        assertThat(parameter.value(), is(nullValue()));
        verifyZeroInteractions(inputLongVarBinaryStream);
    }

    @Test
    public void throws_when_outputLongVarBinaryReader_is_null() {
        try {
            new LongVarBinaryInputOutputParameter<String>(() -> null, null);
            fail("InvalidArgumentException expected");
        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("outputLongVarBinaryReader cannot be null"));
        }
    }

    @Test
    public void throws_when_inputLongVarBinarySupplier_is_null() {
        try {
            new LongVarBinaryInputOutputParameter<String>(null, InputStream -> null);
            fail("InvalidArgumentException expected");

        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("inputLongVarBinarySupplier cannot be null"));
        }
    }

    @Test
    public void binds_results_correctly_when_input_longVarBinary_not_null_and_statement_leaves_scope() throws SQLException, IOException {

        InputStream inputLongVarBinaryStream = mock(InputStream.class);
        Supplier<InputStream> inputLongVarBinarySupplier = () -> inputLongVarBinaryStream;
        String outputLongVarBinaryContent = "this is the content of the out longVarBinary";
        Charset charset = Charset.forName("UTF-8");
        Function<InputStream, String> outputLongVarBinaryReader = inputStream -> readFrom(inputStream, charset);
        LongVarBinaryInputOutputParameter<String> parameter = new LongVarBinaryInputOutputParameter<>(inputLongVarBinarySupplier,
                outputLongVarBinaryReader);

        when(statement.getObject(position)).thenReturn(outputLongVarBinaryContent.getBytes(charset));

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputLongVarBinaryContent));
        InOrder order = inOrder(statement, inputLongVarBinaryStream);
        order.verify(statement).setBinaryStream(position, inputLongVarBinaryStream);
        order.verify(statement).registerOutParameter(position, Types.LONGVARBINARY);
        order.verify(statement).getObject(position);
        order.verify(inputLongVarBinaryStream).close();
    }

    @Test
    public void binds_results_correctly_when_input_longVarBinary_is_null_and_statement_leaves_scope() throws SQLException, IOException {
        String outputLongVarBinaryContent = "this is the content of the out longVarBinary";
        Supplier<InputStream> inputLongVarBinarySupplier = () -> null;
        Charset charset = Charset.forName("UTF-8");
        Function<InputStream, String> outputLongVarBinaryReader = inputStream -> readFrom(inputStream, charset);
        LongVarBinaryInputOutputParameter<String> parameter = new LongVarBinaryInputOutputParameter<>(inputLongVarBinarySupplier,
                outputLongVarBinaryReader);

        when(statement.getObject(position)).thenReturn(outputLongVarBinaryContent.getBytes(charset));

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputLongVarBinaryContent));
        InOrder order = inOrder(statement);
        order.verify(statement).setNull(position, Types.LONGVARBINARY);
        order.verify(statement).registerOutParameter(position, Types.LONGVARBINARY);
        order.verify(statement).getObject(position);
    }

}