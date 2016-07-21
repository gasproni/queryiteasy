package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Time;
import java.sql.Types;

public class TimeOutputParameter implements OutputParameter {
    private Time value = null;

    public Time value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TIME);
            queryScope.add(() -> this.value = statement.getTime(position));
        });
    }

}
