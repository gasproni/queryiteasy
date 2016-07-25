package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Supplier;

public class LongVarBinaryInputOutputParameter implements InputOutputParameter {

    private byte[] value = null;
    private Supplier<InputStream> inputLongVarBinarySupplier;

    public LongVarBinaryInputOutputParameter(Supplier<InputStream> inputLongVarBinarySupplier) {
        if (inputLongVarBinarySupplier == null) {
            throw new InvalidArgumentException("inputLongVarBinarySupplier cannot be null");
        }

        this.inputLongVarBinarySupplier = inputLongVarBinarySupplier;
    }

    public byte[] value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            InputStream inputStream = inputLongVarBinarySupplier.get();
            if (inputStream == null) {
                statement.setNull(position, Types.LONGVARBINARY);
            } else {
                statement.setBinaryStream(position, inputStream);
                queryScope.add(inputStream::close);
            }
            statement.registerOutParameter(position, Types.LONGVARBINARY);
            queryScope.add(() -> value = statement.getBytes(position));
        });
    }
}
