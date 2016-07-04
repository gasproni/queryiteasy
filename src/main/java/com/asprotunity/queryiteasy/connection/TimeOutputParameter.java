package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Time;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asTime;

public class TimeOutputParameter extends OutputParameter<Time>{
    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TIME);
            statementScope.onLeave(() -> setValue(asTime(statement.getObject(position))));
        });
    }
}
