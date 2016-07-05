package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asLong;

public class LongOutputParameter implements OutputParameter {
    private Long value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BIGINT);
            statementScope.onLeave(() -> setValue(asLong(statement.getObject(position))));
        });
    }

    public Long value() {
        return value;
    }

    protected void setValue(Long value) {
        this.value = value;
    }
}
