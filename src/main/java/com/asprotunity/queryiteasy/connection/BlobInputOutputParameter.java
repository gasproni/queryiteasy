package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromBlob;

public class BlobInputOutputParameter<ResultType> implements InputOutputParameter {

    private Function<InputStream, ResultType> outputBlobReader;
    private ResultType value = null;
    private Supplier<InputStream> inputBlobSupplier;

    public BlobInputOutputParameter(Supplier<InputStream> inputBlobSupplier, Function<InputStream, ResultType> outputBlobReader) {
        if (inputBlobSupplier == null) {
            throw new InvalidArgumentException("inputBlobSupplier cannot be null");
        }
        if (outputBlobReader == null) {
            throw new InvalidArgumentException("outputBlobReader cannot be null");
        }

        this.inputBlobSupplier = inputBlobSupplier;
        this.outputBlobReader = outputBlobReader;
    }

    public ResultType value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            InputStream inputStream = inputBlobSupplier.get();
            if (inputStream == null) {
                statement.setNull(position, Types.BLOB);
            } else {
                statement.setBlob(position, inputStream);
                statementScope.add(inputStream::close);
            }
            statement.registerOutParameter(position, Types.BLOB);
            statementScope.add(() -> value = fromBlob(statement.getBlob(position), outputBlobReader));
        });
    }
}
