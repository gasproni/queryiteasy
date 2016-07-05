package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Time;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asTime;

public class TimeOutputParameter implements OutputParameter {
    private Time value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TIME);
            statementScope.onLeave(() -> setValue(asTime(statement.getObject(position))));
        });
    }

    public Time value() {
        return value;
    }

    protected void setValue(Time value) {
        this.value = value;
    }
}
