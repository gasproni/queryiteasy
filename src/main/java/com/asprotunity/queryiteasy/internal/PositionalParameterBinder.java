package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.InputParameterBinder;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.math.BigDecimal;
import java.sql.*;

class PositionalParameterBinder implements InputParameterBinder {

    private final PreparedStatement statement;
    private final int position;

    PositionalParameterBinder(int position, PreparedStatement statement) {
        this.statement = statement;
        this.position = position;
    }

    @Override
    public void bind(String value) {
        RuntimeSQLException.wrapException(() -> statement.setString(this.position, value));
    }

    @Override
    public void bind(Short value) {
        setValue(value, Types.SMALLINT);
    }

    @Override
    public void bind(Integer value) {
        setValue(value, Types.INTEGER);
    }

    @Override
    public void bind(Long value) {
        setValue(value, Types.BIGINT);
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

    @Override
    public void bind(Boolean value) {
        setValue(value, Types.BOOLEAN);
    }

    @Override
    public void bind(BigDecimal value) {
        RuntimeSQLException.wrapException(() -> statement.setBigDecimal(this.position, value));
    }

    @Override
    public void bind(Date value) {
        RuntimeSQLException.wrapException(() -> statement.setDate(this.position, value));
    }

    @Override
    public void bind(Time value) {
        RuntimeSQLException.wrapException(() -> statement.setTime(this.position, value));
    }

    @Override
    public void bind(Timestamp value) {
        RuntimeSQLException.wrapException(() -> statement.setTimestamp(this.position, value));
    }

    private void setValue(Object value, int sqlType) {
        RuntimeSQLException.wrapException(() -> statement.setObject(this.position, value, sqlType));
    }
}
