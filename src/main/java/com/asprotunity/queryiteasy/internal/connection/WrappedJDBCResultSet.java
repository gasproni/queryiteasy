package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class WrappedJDBCResultSet implements ResultSetWrapper {
    private final ResultSet resultSet;
    private final ResultSetMetaData metadata;

    public WrappedJDBCResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.metadata = RuntimeSQLException.executeAndReturnResult(resultSet::getMetaData);
    }

    @Override
    public boolean next() {
        return RuntimeSQLException.executeAndReturnResult(resultSet::next);
    }

    @Override
    public int columnCount() {
        return RuntimeSQLException.executeAndReturnResult(metadata::getColumnCount);
    }

    @Override
    public Object getObject(int columnIndex) {
        return RuntimeSQLException.executeAndReturnResult(() -> resultSet.getObject(columnIndex));
    }

    @Override
    public String columnLabel(int columnIndex) {
        return RuntimeSQLException.executeAndReturnResult(() -> metadata.getColumnLabel(columnIndex));
    }
}
