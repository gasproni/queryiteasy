package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class IntegerInputOutputParameter implements InputOutputParameter {

    private Integer value = null;

    public IntegerInputOutputParameter(Integer value) {
        this.value = value;
    }

    public Integer value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.INTEGER);
            statement.registerOutParameter(position, Types.INTEGER);
            queryScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getInt));
        });
    }

}
