package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.AutoCloseableScope;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.function.Function;

public final class SQLDataConverters {

    public static <ResultType> ResultType fromBlob(Blob blob,
                                                   Function<InputStream, ResultType> blobReader) {
        if (blob == null) {
            return blobReader.apply(null);
        } else {
            try (AutoCloseableScope scope = new AutoCloseableScope();
                 InputStream inputStream = blob.getBinaryStream()) {
                scope.add(blob::free);
                return blobReader.apply(inputStream);
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    public static <ResultType> ResultType fromClob(Clob clob,
                                                   Function<Reader, ResultType> clobReader) {
        if (clob == null) {
            return clobReader.apply(null);
        } else {
            try (AutoCloseableScope scope = new AutoCloseableScope();
                 Reader reader = clob.getCharacterStream()) {
                scope.add(clob::free);
                return clobReader.apply(reader);
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    public static <ResultType> ResultType fromInputStream(InputStream inputStream,
                                                          Function<InputStream, ResultType> inputStreamReader) {
        if (inputStream == null) {
            return null;
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(inputStream::close);
            return inputStreamReader.apply(inputStream);
        }
    }

    public static <ResultType> ResultType fromReader(Reader reader,
                                                     Function<Reader, ResultType> inputStreamReader) {
        if (reader == null) {
            return null;
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(reader::close);
            return inputStreamReader.apply(reader);
        }
    }

}
