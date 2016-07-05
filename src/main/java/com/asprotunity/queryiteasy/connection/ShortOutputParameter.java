package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asShort;

public class ShortOutputParameter implements OutputParameter {
    private Short value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.SMALLINT);
            statementScope.onLeave(() -> setValue(asShort(statement.getObject(position))));
        });
    }

    public Short value() {
        return value;
    }

    protected void setValue(Short value) {
        this.value = value;
    }
}
