package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asBigDecimal;

public class BigDecimalOutputParameter implements OutputParameter {
    private BigDecimal value = null;

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DECIMAL);
            statementScope.onLeave(() -> setValue(asBigDecimal(statement.getObject(position))));
        });
    }

    public BigDecimal value() {
        return value;
    }

    protected void setValue(BigDecimal value) {
        this.value = value;
    }
}
