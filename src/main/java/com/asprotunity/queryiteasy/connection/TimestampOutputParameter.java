package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Timestamp;
import java.sql.Types;

public class TimestampOutputParameter implements OutputParameter {
    private Timestamp value = null;

    public Timestamp value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TIMESTAMP);
            statementScope.add(() -> this.value = statement.getTimestamp(position));
        });
    }

}
