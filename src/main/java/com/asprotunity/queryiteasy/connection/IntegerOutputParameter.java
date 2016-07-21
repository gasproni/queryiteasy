package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class IntegerOutputParameter implements OutputParameter {

    private Integer value = null;

    public Integer value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.INTEGER);
            statementScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getInt));
        });
    }

}
