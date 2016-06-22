package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringOutputParameter extends OutputParameter<String> {

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.VARCHAR);
            statementScope.onLeave(() -> setValue(statement.getString(position)));
        });
    }
}
