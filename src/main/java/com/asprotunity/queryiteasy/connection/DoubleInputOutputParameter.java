package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class DoubleInputOutputParameter implements InputOutputParameter {
    private Double value = null;

    public DoubleInputOutputParameter(Double value) {
        this.value = value;
    }

    public Double value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.DOUBLE);
            statement.registerOutParameter(position, Types.DOUBLE);
            queryScope.add(() -> this.value = SQLValueReaders.returnValueOrNull(statement, position, CallableStatement::getDouble));
        });
    }

}
