package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromLongVarbinary;

public class LongVarBinaryInputOutputParameter<ResultType> implements InputOutputParameter {

    private Function<InputStream, ResultType> outputLongVarBinaryReader;
    private ResultType value = null;
    private Supplier<InputStream> inputLongVarBinarySupplier;

    public LongVarBinaryInputOutputParameter(Supplier<InputStream> inputLongVarBinarySupplier,
                                             Function<InputStream, ResultType> outputLongVarBinaryReader) {
        if (inputLongVarBinarySupplier == null) {
            throw new InvalidArgumentException("inputLongVarBinarySupplier cannot be null");
        }
        if (outputLongVarBinaryReader == null) {
            throw new InvalidArgumentException("outputLongVarBinaryReader cannot be null");
        }

        this.inputLongVarBinarySupplier = inputLongVarBinarySupplier;
        this.outputLongVarBinaryReader = outputLongVarBinaryReader;
    }

    public ResultType value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            InputStream inputStream = inputLongVarBinarySupplier.get();
            if (inputStream == null) {
                statement.setNull(position, Types.LONGVARBINARY);
            } else {
                statement.setBinaryStream(position, inputStream);
                statementScope.onLeave(inputStream::close);
            }
            statement.registerOutParameter(position, Types.LONGVARBINARY);
            statementScope.onLeave(() -> value = fromLongVarbinary(statement.getObject(position), outputLongVarBinaryReader));
        });
    }
}
