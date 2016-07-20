package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Types;

public class BigDecimalOutputParameter implements OutputParameter {
    private BigDecimal value = null;

    public BigDecimal value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.DECIMAL);
            statementScope.add(() -> this.value = statement.getBigDecimal(position));
        });
    }

}
