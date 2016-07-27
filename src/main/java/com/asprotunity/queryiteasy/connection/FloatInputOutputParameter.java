package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class FloatInputOutputParameter implements InputOutputParameter {
    private Float value = null;

    public FloatInputOutputParameter(Float value) {
        this.value = value;
    }

    public Float value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.REAL);
            statement.registerOutParameter(position, Types.REAL);
            queryScope.add(() -> this.value = SQLValueReaders.returnValueOrNull(statement, position, CallableStatement::getFloat));
        });
    }

}
