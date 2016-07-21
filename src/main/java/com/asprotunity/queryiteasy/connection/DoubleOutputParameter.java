package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class DoubleOutputParameter implements OutputParameter {
    private Double value = null;

    public Double value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DOUBLE);
            statementScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getDouble));
        });
    }

}
