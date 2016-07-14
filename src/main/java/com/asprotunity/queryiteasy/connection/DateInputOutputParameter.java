package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDate;

public class DateInputOutputParameter implements InputOutputParameter {
    private Date value = null;

    public DateInputOutputParameter(Date value) {
        this.value = value;
    }

    public Date value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setDate(position, value);
            statement.registerOutParameter(position, Types.DATE);
            statementScope.add(() -> this.value = asDate(statement.getObject(position)));
        });
    }

}
