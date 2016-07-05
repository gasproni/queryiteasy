package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringOutputParameter implements OutputParameter {

    private String value = null;

    public String value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.VARCHAR);
            statementScope.onLeave(() -> this.value = statement.getString(position));
        });
    }

}
