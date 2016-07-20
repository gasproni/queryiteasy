package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class LongVarBinaryOutputParameter implements OutputParameter {

    private byte[] value = null;

    public byte[] value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.LONGVARBINARY);
            statementScope.add(() -> value = statement.getBytes(position));
        });
    }
}
