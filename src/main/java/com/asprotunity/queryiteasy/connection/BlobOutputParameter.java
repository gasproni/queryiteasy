package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.connection.BlobReaders.fromBlob;

public class BlobOutputParameter<ResultType> implements OutputParameter {

    private Function<InputStream, ResultType> blobReader;
    private ResultType value = null;

    public BlobOutputParameter(Function<InputStream, ResultType> blobReader) {
        if (blobReader == null) {
            throw new InvalidArgumentException("blobReader cannot be null");
        }
        this.blobReader = blobReader;
    }

    public ResultType value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BLOB);
            queryScope.add(() -> value = fromBlob(statement.getBlob(position), blobReader));
        });
    }
}
