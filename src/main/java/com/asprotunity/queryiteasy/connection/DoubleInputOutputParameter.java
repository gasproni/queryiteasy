package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDouble;

public class DoubleInputOutputParameter implements InputOutputParameter {
    private Double value = null;

    public DoubleInputOutputParameter(Double value) {
        this.value = value;
    }

    public Double value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.DOUBLE);
            statement.registerOutParameter(position, Types.DOUBLE);
            statementScope.add(() -> this.value = asDouble(statement.getObject(position)));
        });
    }

}
