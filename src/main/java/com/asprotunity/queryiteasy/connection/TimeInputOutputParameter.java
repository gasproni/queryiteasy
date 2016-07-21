package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Time;
import java.sql.Types;

public class TimeInputOutputParameter implements InputOutputParameter {
    private Time value = null;

    public TimeInputOutputParameter(Time value) {
        this.value = value;
    }

    public Time value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setTime(position, this.value);
            statement.registerOutParameter(position, Types.TIME);
            queryScope.add(() -> this.value = statement.getTime(position));
        });
    }

}
