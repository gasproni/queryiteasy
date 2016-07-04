package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDate;

public class DateOutputParameter extends OutputParameter<Date> {
    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DATE);
            statementScope.onLeave(() -> setValue(asDate(statement.getObject(position))));
        });
    }
}
