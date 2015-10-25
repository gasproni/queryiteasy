package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.connection.Row;

import java.sql.ResultSet;

public class WrappedResultSet implements Row {


    private final ResultSet resultSet;

    public WrappedResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public int getInt(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> this.resultSet.getInt(columnName));
    }

    @Override
    public String getString(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> this.resultSet.getString(columnName));
    }
}
