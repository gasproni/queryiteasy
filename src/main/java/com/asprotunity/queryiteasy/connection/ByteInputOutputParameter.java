package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class ByteInputOutputParameter implements InputOutputParameter {
    private Byte value = null;

    public ByteInputOutputParameter(Byte value) {
        this.value = value;
    }

    public Byte value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.TINYINT);
            statement.registerOutParameter(position, Types.TINYINT);
            statementScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getByte));
        });
    }

}
