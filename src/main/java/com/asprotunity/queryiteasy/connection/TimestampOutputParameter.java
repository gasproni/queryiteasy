package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Timestamp;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asTimestamp;

public class TimestampOutputParameter implements OutputParameter {
    private Timestamp value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TIMESTAMP);
            statementScope.onLeave(() -> setValue(asTimestamp(statement.getObject(position))));
        });
    }

    public Timestamp value() {
        return value;
    }

    protected void setValue(Timestamp value) {
        this.value = value;
    }
}
