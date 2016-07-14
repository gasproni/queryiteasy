package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDate;

public class DateOutputParameter implements OutputParameter {
    private Date value = null;

    public Date value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DATE);
            statementScope.add(() -> this.value = asDate(statement.getObject(position)));
        });
    }

}
