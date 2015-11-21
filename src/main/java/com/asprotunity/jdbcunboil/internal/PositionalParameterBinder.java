package com.asprotunity.jdbcunboil.internal;

import com.asprotunity.jdbcunboil.connection.StatementParameterBinder;
import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;

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
        RuntimeSQLException.wrapException(() -> {
            if (value != null) {
                statement.setInt(this.position, value);
            } else {
                statement.setNull(this.position, Types.INTEGER);
            }

        });
    }

    @Override
    public void bind(Double value) {
        RuntimeSQLException.wrapException(() -> statement.setDouble(this.position, value));
    }

}
