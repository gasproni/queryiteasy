package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDouble;

public class DoubleOutputParameter implements OutputParameter {
    private Double value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DOUBLE);
            statementScope.onLeave(() -> setValue(asDouble(statement.getObject(position))));
        });
    }

    public Double value() {
        return value;
    }

    protected void setValue(Double value) {
        this.value = value;
    }
}
