package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asBoolean;

public class BooleanOutputParameter extends OutputParameter<Boolean> {
    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BOOLEAN);
            statementScope.onLeave(() -> setValue(asBoolean(statement.getObject(position))));
        });
    }
}
