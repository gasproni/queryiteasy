package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asBigDecimal;

public class BigDecimalInputOutputParameter implements InputOutputParameter {
    private BigDecimal value = null;

    public BigDecimalInputOutputParameter(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.setBigDecimal(position, this.value);
            statement.registerOutParameter(position, Types.DECIMAL);
            statementScope.add(() -> this.value = asBigDecimal(statement.getObject(position)));
        });
    }

}
