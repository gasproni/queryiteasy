package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.stringio.StringIO;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromBlob;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromClob;
import static com.asprotunity.queryiteasy.stringio.StringIO.readFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class SQLDataConvertersTest {

    @Test
    public void from_blob_converts_from_sql_blob() throws IOException, SQLException {
        Blob blob = mock(Blob.class);
        String expected = "this is a string to be converted;";
        Charset charset = Charset.forName("UTF-8");
        when(blob.getBinaryStream()).thenReturn(new ByteArrayInputStream(expected.getBytes("UTF-8")));

        String converted = fromBlob(blob, inputStream -> readFrom(inputStream, charset));

        assertThat(converted, is(expected));
    }

    @Test
    public void from_blob_frees_resources() throws IOException, SQLException {
        Blob blob = mock(Blob.class);
        ByteArrayInputStream blobInputStream = mock(ByteArrayInputStream.class);
        when(blob.getBinaryStream()).thenReturn(blobInputStream);

        fromBlob(blob, inputStream -> "doesn't matter");

        InOrder order = inOrder(blob, blobInputStream);
        order.verify(blob, times(1)).getBinaryStream();
        order.verify(blobInputStream, times(1)).close();
        order.verify(blob, times(1)).free();
    }

    @Test
    public void returns_null_if_blob_is_null() throws UnsupportedEncodingException, SQLException {
        String converted = fromBlob(null, inputStream -> {
            if (inputStream == null)
                return null;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(nullValue()));
    }

    @Test
    public void from_clob_converts_from_sql_clob() throws UnsupportedEncodingException, SQLException {
        Clob clob = mock(Clob.class);
        String expected = "this is a string to be converted;";
        when(clob.getCharacterStream()).thenReturn(new StringReader(expected));

        String converted = fromClob(clob, StringIO::readFrom);

        assertThat(converted, is(expected));
    }

    @Test
    public void from_clob_frees_resources() throws IOException, SQLException {
        Clob clob = mock(Clob.class);
        Reader reader = mock(Reader.class);
        when(clob.getCharacterStream()).thenReturn(reader);

        fromClob(clob, stream -> "doesn't matter");

        InOrder order = inOrder(clob, reader);
        order.verify(clob, times(1)).getCharacterStream();
        order.verify(reader, times(1)).close();
        order.verify(clob, times(1)).free();
    }

    @Test
    public void returns_null_if_clob_is_null() throws UnsupportedEncodingException, SQLException {
        String converted = fromClob(null, reader -> {
            if (reader == null)
                return null;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(nullValue()));
    }

}