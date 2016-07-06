package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class BlobOutputParameterTest extends OutputParameterTestBase {

    @Test(expected = InvalidArgumentException.class)
    public void throws_when_blobreader_is_null() {
        new BlobOutputParameter<String>(null);
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {

        String blobContent = "this is the content of the blob";
        Function<InputStream, String> blobReader = inputStream -> blobContent;
        BlobOutputParameter<String> parameter = new BlobOutputParameter<>(blobReader);

        Blob value = mock(Blob.class);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(blobContent));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.BLOB);
        order.verify(statement).getObject(position);
    }

}