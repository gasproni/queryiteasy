package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.Types;

public class DateOutputParameter implements OutputParameter {
    private Date value = null;

    public Date value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DATE);
            queryScope.add(() -> this.value = statement.getDate(position));
        });
    }

}
