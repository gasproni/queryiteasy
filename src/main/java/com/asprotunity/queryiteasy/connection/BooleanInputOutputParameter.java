package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class BooleanInputOutputParameter implements InputOutputParameter {
    private Boolean value = null;

    public BooleanInputOutputParameter(Boolean value) {
        this.value = value;
    }

    public Boolean value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.BOOLEAN);
            statement.registerOutParameter(position, Types.BOOLEAN);
            queryScope.add(() ->
                    this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getBoolean)
            );
        });
    }

}
