package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class BlobInputOutputParameterTest extends OutputParameterTestBase {

    @Test
    public void value_is_initialized_with_null() {
        InputStream inputBlobStream = mock(InputStream.class);
        BlobInputOutputParameter<String> parameter = new BlobInputOutputParameter<>(() -> inputBlobStream,
                outputBlobStream -> "doesn't matter");

        assertThat(parameter.value(), is(nullValue()));
        verifyZeroInteractions(inputBlobStream);
    }

    @Test
    public void throws_when_outputBlobReader_is_null() {
        try {
            new BlobInputOutputParameter<String>(() -> null, null);
            fail("InvalidArgumentException expected");
        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("outputBlobReader cannot be null."));
        }
    }

    @Test
    public void throws_when_inputBlobSupplier_is_null() {
        try {
            new BlobInputOutputParameter<String>(null, InputStream -> null);
            fail("InvalidArgumentException expected");

        } catch (InvalidArgumentException e) {
            assertThat(e.getMessage(), is("inputBlobSupplier cannot be null."));
        }
    }

    @Test
    public void binds_results_correctly_when_input_blob_not_null_and_statement_leaves_scope() throws SQLException, IOException {

        String outputBlobContent = "this is the content of the blob";
        InputStream inputBlobStream = mock(InputStream.class);
        Supplier<InputStream> inputBlobSupplier = () -> inputBlobStream;
        Function<InputStream, String> outputBlobReader = inputStream -> outputBlobContent;
        BlobInputOutputParameter<String> parameter = new BlobInputOutputParameter<>(inputBlobSupplier, outputBlobReader);

        Blob value = mock(Blob.class);
        when(statement.getBlob(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(outputBlobContent));
        InOrder order = inOrder(statement, inputBlobStream);
        order.verify(statement).setBlob(position, inputBlobStream);
        order.verify(statement).registerOutParameter(position, Types.BLOB);
        order.verify(statement).getBlob(position);
        order.verify(inputBlobStream).close();
    }

    @Test
    public void binds_results_correctly_when_input_blob_is_null_and_statement_leaves_scope() throws SQLException, IOException {

        String blobContent = "this is the content of the blob";
        Supplier<InputStream> inputBlobSupplier = () -> null;
        Function<InputStream, String> outputBlobReader = inputStream -> blobContent;
        BlobInputOutputParameter<String> parameter = new BlobInputOutputParameter<>(inputBlobSupplier, outputBlobReader);

        Blob value = mock(Blob.class);
        when(statement.getBlob(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(blobContent));
        InOrder order = inOrder(statement);
        order.verify(statement).setNull(position, Types.BLOB);
        order.verify(statement).registerOutParameter(position, Types.BLOB);
        order.verify(statement).getBlob(position);
    }

}