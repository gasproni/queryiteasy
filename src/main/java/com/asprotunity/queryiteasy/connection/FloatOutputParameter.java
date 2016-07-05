package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asFloat;

public class FloatOutputParameter implements OutputParameter {
    private Float value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.REAL);
            statementScope.onLeave(() -> setValue(asFloat(statement.getObject(position))));
        });
    }

    public Float value() {
        return value;
    }

    protected void setValue(Float value) {
        this.value = value;
    }
}
