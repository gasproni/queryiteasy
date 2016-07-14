package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asFloat;

public class FloatInputOutputParameter implements InputOutputParameter {
    private Float value = null;

    public FloatInputOutputParameter(Float value) {
        this.value = value;
    }

    public Float value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.REAL);
            statement.registerOutParameter(position, Types.REAL);
            statementScope.add(() -> this.value = asFloat(statement.getObject(position)));
        });
    }

}
