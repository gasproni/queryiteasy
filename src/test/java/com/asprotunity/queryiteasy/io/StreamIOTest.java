package com.asprotunity.queryiteasy.io;

import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.SQLException;

import static com.asprotunity.queryiteasy.io.StringIO.readFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class StreamIOTest {

    @Test
    public void from_inputstream_reader_frees_resources() throws IOException, SQLException {
        InputStream inputStream = mock(InputStream.class);
        StreamIO.fromInputStream(inputStream, is -> "doesn't matter");
        verify(inputStream, times(1)).close();
    }

    @Test
    public void from_inputstream_reads_stream() throws IOException, SQLException {
        String expected = "this is a string to be converted;";
        Charset charset = Charset.forName("UTF-8");

        String converted = StreamIO.fromInputStream(new ByteArrayInputStream(expected.getBytes("UTF-8")),
                inputStream -> readFrom(inputStream, charset));

        assertThat(converted, is(expected));

    }

    @Test
    public void from_inputstream_passes_null_stream_to_reader() throws IOException, SQLException {
        String expectedIfNull = "Hey, the stream is null!";
        String converted = StreamIO.fromInputStream(null, inputStream -> {
            if (inputStream == null)
                return expectedIfNull;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(expectedIfNull));

    }

    @Test
    public void from_reader_frees_resources() throws IOException, SQLException {
        Reader reader = mock(Reader.class);
        StreamIO.fromReader(reader, rd -> "doesn't matter");
        verify(reader, times(1)).close();
    }

    @Test
    public void from_reader_converts_from_reader() throws UnsupportedEncodingException, SQLException {
        String expected = "this is a string to be converted;";
        Reader reader = new StringReader(expected);
        String converted = StreamIO.fromReader(reader, StringIO::readFrom);

        assertThat(converted, is(expected));
    }

    @Test
    public void from_reader_passes_null_stream_to_input_reader() throws IOException, SQLException {
        String expectedIfNull = "Hey, the reader is null!";
        String converted = StreamIO.fromReader(null, reader -> {
            if (reader == null)
                return expectedIfNull;
            else throw new RuntimeException("Shouldn't get here!");
        });

        assertThat(converted, is(expectedIfNull));

    }
}