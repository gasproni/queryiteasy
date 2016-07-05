package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringInputOutputParameter implements InputOutputParameter {

    private String value = null;

    public StringInputOutputParameter(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setString(position, value());
            statement.registerOutParameter(position, Types.VARCHAR);
            statementScope.onLeave(() -> value = statement.getString(position));
        });
    }

}
