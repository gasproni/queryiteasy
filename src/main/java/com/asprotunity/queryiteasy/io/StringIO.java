package com.asprotunity.queryiteasy.io;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;

public abstract class StringIO {

    public static String readFrom(InputStream inputStream, Charset charset) {
        if (inputStream == null) {
            return null;
        }
        return new java.util.Scanner(inputStream, charset.name()).useDelimiter("\\A").next();
    }

    public static String readFrom(Reader reader) {
        if (reader == null) {
            return null;
        }
        return new java.util.Scanner(reader).useDelimiter("\\A").next();
    }
}
