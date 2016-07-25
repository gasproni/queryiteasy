package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Types;

public class BigDecimalInputOutputParameter implements InputOutputParameter {
    private BigDecimal value = null;

    public BigDecimalInputOutputParameter(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope queryScope) {
        RuntimeSQLException.execute(() -> {
            statement.setBigDecimal(position, this.value);
            statement.registerOutParameter(position, Types.DECIMAL);
            queryScope.add(() -> this.value = statement.getBigDecimal(position));
        });
    }

}
