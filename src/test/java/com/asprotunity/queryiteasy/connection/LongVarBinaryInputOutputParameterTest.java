package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class LongVarBinaryInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_null() {
        InputStream inputLongVarBinaryStream = mock(InputStream.class);
        LongVarBinaryInputOutputParameter parameter =
                new LongVarBinaryInputOutputParameter(() -> inputLongVarBinaryStream);

        assertThat(parameter.value(), is(nullValue()));
        verifyZeroInteractions(inputLongVarBinaryStream);
    }

    @Test
    public void throws_when_inputLongVarBinarySupplier_is_null() {
        try {
            new LongVarBinaryInputOutputParameter(null);
            fail("InvalidArgumentException expected");

        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("inputLongVarBinarySupplier cannot be null"));
        }
    }

    @Test
    public void binds_results_correctly_when_input_longVarBinary_not_null_and_statement_leaves_scope() throws SQLException, IOException {

        InputStream inputLongVarBinaryStream = mock(InputStream.class);
        Supplier<InputStream> inputLongVarBinarySupplier = () -> inputLongVarBinaryStream;
        byte[] outputLongVarBinaryContent = new byte[]{1, 2, 3, 4, 5, 6, 7};
        LongVarBinaryInputOutputParameter parameter = new LongVarBinaryInputOutputParameter(inputLongVarBinarySupplier);

        when(statement.getBytes(position)).thenReturn(outputLongVarBinaryContent);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputLongVarBinaryContent));
        InOrder order = inOrder(statement, inputLongVarBinaryStream);
        order.verify(statement).setBinaryStream(position, inputLongVarBinaryStream);
        order.verify(statement).registerOutParameter(position, Types.LONGVARBINARY);
        order.verify(statement).getBytes(position);
        order.verify(inputLongVarBinaryStream).close();
    }

    @Test
    public void binds_results_correctly_when_input_longVarBinary_is_null_and_statement_leaves_scope() throws SQLException, IOException {
        byte[] outputLongVarBinaryContent = new byte[]{1, 2, 3, 4, 5, 6, 7};
        Supplier<InputStream> inputLongVarBinarySupplier = () -> null;
        LongVarBinaryInputOutputParameter parameter = new LongVarBinaryInputOutputParameter(inputLongVarBinarySupplier);

        when(statement.getBytes(position)).thenReturn(outputLongVarBinaryContent);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputLongVarBinaryContent));
        InOrder order = inOrder(statement);
        order.verify(statement).setNull(position, Types.LONGVARBINARY);
        order.verify(statement).registerOutParameter(position, Types.LONGVARBINARY);
        order.verify(statement).getBytes(position);
    }

}