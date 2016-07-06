package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asBoolean;

public class BooleanInputOutputParameter implements InputOutputParameter {
    private Boolean value = null;

    public BooleanInputOutputParameter(Boolean value) {
        this.value = value;
    }

    public Boolean value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.BOOLEAN);
            statement.registerOutParameter(position, Types.BOOLEAN);
            statementScope.onLeave(() -> this.value = asBoolean(statement.getObject(position)));
        });
    }

}
