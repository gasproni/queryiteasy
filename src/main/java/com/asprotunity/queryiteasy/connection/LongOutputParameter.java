package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class LongOutputParameter implements OutputParameter {
    private Long value = null;

    public Long value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.BIGINT);
            queryScope.add(() -> this.value = SQLResultReaders.returnValueOrNull(statement, position, CallableStatement::getLong));
        });
    }

}
