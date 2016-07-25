package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class ShortInputOutputParameter implements InputOutputParameter {
    private Short value = null;

    public ShortInputOutputParameter(Short value) {
        this.value = value;
    }

    public Short value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.SMALLINT);
            statement.registerOutParameter(position, Types.SMALLINT);
            queryScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getShort));
        });
    }

}
