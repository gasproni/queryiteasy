package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.io.StringIO;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import static com.asprotunity.queryiteasy.connection.BlobReaders.fromBlob;
import static com.asprotunity.queryiteasy.connection.BlobReaders.fromClob;
import static com.asprotunity.queryiteasy.io.StringIO.readFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class BlobReadersTest {

    @Test
    public void from_blob_reads_from_blob_inputstream() throws IOException, SQLException {
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
    public void if_blob_null_passes_null_stream_to_reader() throws UnsupportedEncodingException, SQLException {
        String expectedIfNull = "Hey, blob is null!";
        String converted = fromBlob(null, inputStream -> {
            if (inputStream == null)
                return expectedIfNull;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(expectedIfNull));
    }

    @Test
    public void from_clob_reads_from_clob_reader() throws UnsupportedEncodingException, SQLException {
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
    public void if_clob_null_passes_null_reader_to_clob_reader() throws UnsupportedEncodingException, SQLException {
        String expectedIfNull = "Hey, clob is null!";
        String converted = fromClob(null, reader -> {
            if (reader == null)
                return expectedIfNull;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(expectedIfNull));
    }


}