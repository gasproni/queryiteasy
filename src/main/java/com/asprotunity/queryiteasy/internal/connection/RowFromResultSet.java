package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class RowFromResultSet implements Row {

    private ResultSet resultSet;
    private ResultSetMetaData metaData;

    public RowFromResultSet(ResultSet resultSet) {
        RuntimeSQLException.execute(() -> {
            this.resultSet = resultSet;
            metaData = resultSet.getMetaData();
        });
    }

    @Override
    public Object at(String columnLabel) {
        return RuntimeSQLException.executeAndReturnResult(() -> resultSet.getObject(columnLabel));
    }

    @Override
    public Object at(int columnPosition) {
        return RuntimeSQLException.executeAndReturnResult(() -> resultSet.getObject(columnPosition));
    }

    @Override
    public InputStream binaryStreamAt(String columnLabel) {
        return RuntimeSQLException.executeAndReturnResult(() -> resultSet.getBinaryStream(columnLabel));
    }

    @Override
    public InputStream binaryStreamAt(int columnPosition) {
        return RuntimeSQLException.executeAndReturnResult(() -> resultSet.getBinaryStream(columnPosition));
    }

    @Override
    public int columnCount() {
        return RuntimeSQLException.executeAndReturnResult(() -> metaData.getColumnCount());
    }

}
