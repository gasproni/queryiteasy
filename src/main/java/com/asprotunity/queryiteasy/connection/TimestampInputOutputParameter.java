package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Timestamp;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asTimestamp;

public class TimestampInputOutputParameter implements InputOutputParameter {
    private Timestamp value = null;

    public TimestampInputOutputParameter(Timestamp value) {
        this.value = value;
    }

    public Timestamp value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setTimestamp(position, this.value);
            statement.registerOutParameter(position, Types.TIMESTAMP);
            statementScope.onLeave(() -> this.value = asTimestamp(statement.getObject(position)));
        });
    }

}