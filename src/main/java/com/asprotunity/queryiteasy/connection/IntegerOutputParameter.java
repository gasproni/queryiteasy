package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asInteger;

public class IntegerOutputParameter extends OutputParameter<Integer> {

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.INTEGER);
            statementScope.onLeave(() -> setValue(asInteger(statement.getObject(position))));
        });
    }
}
