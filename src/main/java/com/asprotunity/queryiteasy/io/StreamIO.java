package com.asprotunity.queryiteasy.io;

import com.asprotunity.queryiteasy.scope.AutoCloseableScope;

import java.io.InputStream;
import java.io.Reader;
import java.util.function.Function;

public class StreamIO {
    public static <ResultType> ResultType fromInputStream(InputStream inputStream,
                                                          Function<InputStream, ResultType> inputStreamReader) {
        if (inputStream == null) {
            return inputStreamReader.apply(null);
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(inputStream::close);
            return inputStreamReader.apply(inputStream);
        }
    }

    public static <ResultType> ResultType fromReader(Reader reader,
                                                     Function<Reader, ResultType> inputStreamReader) {
        if (reader == null) {
            return inputStreamReader.apply(null);
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(reader::close);
            return inputStreamReader.apply(reader);
        }
    }
}
