package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class LongInputOutputParameter implements InputOutputParameter {

    private Long value = null;

    public LongInputOutputParameter(Long value) {
        this.value = value;
    }

    public Long value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setObject(position, this.value, Types.BIGINT);
            statement.registerOutParameter(position, Types.BIGINT);
            queryScope.add(() -> this.value = Parameter.returnValueOrNull(statement, position, CallableStatement::getLong));
        });
    }

}
