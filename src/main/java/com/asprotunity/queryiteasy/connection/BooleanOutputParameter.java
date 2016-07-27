package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class BooleanOutputParameter implements OutputParameter {
    private Boolean value = null;

    public Boolean value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BOOLEAN);
            queryScope.add(() -> this.value = SQLValueReaders.returnValueOrNull(statement, position, CallableStatement::getBoolean));
        });
    }

}
