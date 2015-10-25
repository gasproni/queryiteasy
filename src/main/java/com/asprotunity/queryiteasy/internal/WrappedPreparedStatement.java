package com.asprotunity.queryiteasy.internal;


import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.connection.Statement;

import java.sql.PreparedStatement;

public class WrappedPreparedStatement implements Statement {

    private PreparedStatement preparedStatement;

    public WrappedPreparedStatement(PreparedStatement preparedStatement) {
        if (preparedStatement == null) {
            throw new NullPointerException("preparedStatement cannot be null");
        }
        this.preparedStatement = preparedStatement;
    }

    @Override
    public void execute() {
        RuntimeSQLException.wrapException(preparedStatement::execute);
    }

    @Override
    public void setString(int position, String value) {
        RuntimeSQLException.wrapException(() ->
                preparedStatement.setString(position, value));
    }

    @Override
    public void setInt(int position, int value) {
        RuntimeSQLException.wrapException(() ->
                preparedStatement.setInt(position, value));
    }

    @Override
    public void setDouble(int position, double value) {
        RuntimeSQLException.wrapException(() ->
                preparedStatement.setDouble(position, value));
    }
}
