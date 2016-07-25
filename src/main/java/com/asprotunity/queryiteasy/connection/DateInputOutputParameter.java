package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Types;

public class DateInputOutputParameter implements InputOutputParameter {
    private Date value = null;

    public DateInputOutputParameter(Date value) {
        this.value = value;
    }

    public Date value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setDate(position, value);
            statement.registerOutParameter(position, Types.DATE);
            queryScope.add(() -> this.value = statement.getDate(position));
        });
    }

}
