package com.asprotunity.jdbcunboil.internal;

import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;
import com.asprotunity.jdbcunboil.row.Row;

import java.sql.ResultSet;

public class WrappedResultSet implements Row {


    private final ResultSet resultSet;

    public WrappedResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public Integer getInteger(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            int result = this.resultSet.getInt(columnName);
            if (this.resultSet.wasNull()) {
                return null;
            }
            return result;
        });
    }

    @Override
    public String getString(String columnName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> this.resultSet.getString(columnName));
    }
}
