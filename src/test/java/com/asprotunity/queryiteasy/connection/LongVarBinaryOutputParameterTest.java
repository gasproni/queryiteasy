package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.stringio.StringIO.readFrom;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

public class LongVarBinaryOutputParameterTest extends OutputParameterTestBase {

    @Test(expected = InvalidArgumentException.class)
    public void throws_when_binaryreader_is_null() {
        new LongVarBinaryOutputParameter<String>(null);
    }

    @Test
    public void binds_results_correctly_when_statement_leaves_scope() throws SQLException {

        String binaryContent = "this is the content of the binary";
        Charset charset = Charset.forName("UTF-8");
        Function<InputStream, String> binaryReader = inputStream -> readFrom(inputStream, charset);
        LongVarBinaryOutputParameter<String> parameter = new LongVarBinaryOutputParameter<>(binaryReader);

        byte[] value = binaryContent.getBytes(charset);
        when(statement.getObject(position)).thenReturn(value);

        bindParameterAndEmulateCall(parameter);

        assertThat(parameter.value(), is(binaryContent));
        InOrder order = inOrder(statement);
        order.verify(statement).registerOutParameter(position, Types.LONGVARBINARY);
        order.verify(statement).getObject(position);
    }

}