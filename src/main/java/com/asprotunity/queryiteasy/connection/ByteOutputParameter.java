package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asByte;

public class ByteOutputParameter implements OutputParameter {
    private Byte value = null;

    public Byte value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.TINYINT);
            statementScope.onLeave(() -> this.value = asByte(statement.getObject(position)));
        });
    }

}
