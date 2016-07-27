package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.connection.BlobReaders.fromClob;

public class ClobInputOutputParameter<ResultType> implements InputOutputParameter {

    private Function<Reader, ResultType> outputClobReader;
    private ResultType value = null;
    private Supplier<Reader> inputClobSupplier;

    public ClobInputOutputParameter(Supplier<Reader> inputClobSupplier, Function<Reader, ResultType> outputClobReader) {
        InvalidArgumentException.throwIfNull(inputClobSupplier, "inputClobSupplier");
        InvalidArgumentException.throwIfNull(outputClobReader, "outputClobReader");

        this.inputClobSupplier = inputClobSupplier;
        this.outputClobReader = outputClobReader;
    }

    public ResultType value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            Reader reader = inputClobSupplier.get();
            if (reader == null) {
                statement.setNull(position, Types.CLOB);
            } else {
                statement.setClob(position, reader);
                queryScope.add(reader::close);
            }
            statement.registerOutParameter(position, Types.CLOB);
            queryScope.add(() -> value = fromClob(statement.getClob(position), outputClobReader));
        });
    }
}
