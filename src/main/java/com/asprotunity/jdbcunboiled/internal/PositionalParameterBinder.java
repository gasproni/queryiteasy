package com.asprotunity.jdbcunboiled.internal;

import com.asprotunity.jdbcunboiled.connection.StatementParameterBinder;
import com.asprotunity.jdbcunboiled.exception.RuntimeSQLException;

import java.sql.PreparedStatement;
import java.sql.Types;

class PositionalParameterBinder implements StatementParameterBinder {

    private PreparedStatement statement;
    private int position;

    PositionalParameterBinder(int position, PreparedStatement statement) {
        this.statement = statement;
        this.position = position;
    }

    @Override
    public void bind(String value) {
        RuntimeSQLException.wrapException(() -> statement.setString(this.position, value));
    }

    @Override
    public void bind(Integer value) {
        setValue(value, Types.INTEGER);
    }

    @Override
    public void bind(Double value) {
        setValue(value, Types.DOUBLE);
    }

    @Override
    public void bind(Float value) {
        setValue(value, Types.REAL);
    }

    @Override
    public void bind(Byte value) {
        setValue(value, Types.TINYINT);
    }

    private void setValue(Object value, int sqlType) {
        RuntimeSQLException.wrapException(() -> statement.setObject(this.position, value, sqlType));
    }
}
