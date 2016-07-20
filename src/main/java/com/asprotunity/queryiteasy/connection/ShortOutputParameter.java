package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.OutputParameter.returnValueOrNull;

public class ShortOutputParameter implements OutputParameter {
    private Short value = null;

    public Short value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.SMALLINT);
            statementScope.add(() -> this.value = returnValueOrNull(statement, position, CallableStatement::getShort));
        });
    }

}
