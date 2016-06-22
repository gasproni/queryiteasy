package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringInputOutputParameter extends InputOutputParameter<String> {

    public StringInputOutputParameter(String value) {
        super(value);
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setString(position, value());
            statement.registerOutParameter(position, Types.VARCHAR);
            statementScope.onLeave(() -> setValue(statement.getString(position)));
        });
    }
}
