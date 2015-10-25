package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.connection.TableRow;

import java.sql.ResultSet;

public class WrappedResultSet implements TableRow {


    private final ResultSet resultSet;

    public WrappedResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public int getInt(String rowName) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> this.resultSet.getInt(rowName));
    }
}
