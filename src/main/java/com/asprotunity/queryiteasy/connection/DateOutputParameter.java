package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDate;

public class DateOutputParameter implements OutputParameter {
    private Date value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DATE);
            statementScope.onLeave(() -> setValue(asDate(statement.getObject(position))));
        });
    }

    public Date value() {
        return value;
    }

    protected void setValue(Date value) {
        this.value = value;
    }
}
