package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class ByteOutputParameter implements OutputParameter {
    private Byte value = null;

    public Byte value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TINYINT);
            statementScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getByte));
        });
    }

}
