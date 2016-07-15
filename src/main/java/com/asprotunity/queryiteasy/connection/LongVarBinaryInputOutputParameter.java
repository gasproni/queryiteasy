package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asByteArray;

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
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            InputStream inputStream = inputLongVarBinarySupplier.get();
            if (inputStream == null) {
                statement.setNull(position, Types.LONGVARBINARY);
            } else {
                statement.setBinaryStream(position, inputStream);
                statementScope.add(inputStream::close);
            }
            statement.registerOutParameter(position, Types.LONGVARBINARY);
            statementScope.add(() -> value = asByteArray(statement.getObject(position)));
        });
    }
}
