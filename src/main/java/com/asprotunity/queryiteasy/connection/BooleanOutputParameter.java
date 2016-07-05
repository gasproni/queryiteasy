package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asBoolean;

public class BooleanOutputParameter implements OutputParameter {
    private Boolean value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BOOLEAN);
            statementScope.onLeave(() -> setValue(asBoolean(statement.getObject(position))));
        });
    }

    public Boolean value() {
        return value;
    }

    protected void setValue(Boolean value) {
        this.value = value;
    }
}
