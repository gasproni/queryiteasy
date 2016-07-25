package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
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
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TIMESTAMP);
            queryScope.add(() -> this.value = statement.getTimestamp(position));
        });
    }

}
