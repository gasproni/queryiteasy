package com.asprotunity.jdbcunboil.internal;

import com.asprotunity.jdbcunboil.connection.Row;
import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class WrappedResultSet implements Row {


    private final ResultSet resultSet;

    public WrappedResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public String getString(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() ->
                resultSet.getString(columnName));
    }

    @Override
    public Integer getInteger(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() ->
                returnResultOrNull(resultSet.getInt(columnName)));
    }

    @Override
    public Double getDouble(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() ->
                returnResultOrNull(resultSet.getDouble(columnName)));
    }

    private <ReturnType> ReturnType returnResultOrNull(ReturnType result) throws SQLException {
        if (this.resultSet.wasNull()) {
            return null;
        }
        return result;
    }
}
