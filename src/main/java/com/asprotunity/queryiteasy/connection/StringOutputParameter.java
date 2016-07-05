package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringOutputParameter implements OutputParameter {

    private String value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.VARCHAR);
            statementScope.onLeave(() -> setValue(statement.getString(position)));
        });
    }

    public String value() {
        return value;
    }

    protected void setValue(String value) {
        this.value = value;
    }
}
