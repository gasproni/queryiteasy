package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class BooleanOutputParameter implements OutputParameter {
    private Boolean value = null;

    public Boolean value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BOOLEAN);
            statementScope.add(() -> this.value = statement.getBoolean(position));
        });
    }

}
