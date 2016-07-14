package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;

public class RowFromResultSet implements Row {

    private Object columns[];
    private HashMap<String, Integer> labelToColumn;

    public RowFromResultSet(ResultSet resultSet) {
        RuntimeSQLException.execute(() -> {
            ResultSetMetaData metadata = resultSet.getMetaData();
            columns = new Object[metadata.getColumnCount()];
            labelToColumn = new HashMap<>();
            for (int columnIndex = 1; columnIndex <= columns.length; ++columnIndex) {
                int position = columnIndex(columnIndex);
                Object object = resultSet.getObject(columnIndex);
                columns[position] = object;
                labelToColumn.put(normaliseColumnLabel(metadata.getColumnLabel(columnIndex)), columnIndex);
            }
        });
    }

    @Override
    public Object at(String columnLabel) {
        return at(columnPosition(columnLabel));
    }

    @Override
    public Object at(int columnPosition) {
        return columns[columnIndex(columnPosition)];
    }

    @Override
    public int columnCount() {
        return columns.length;
    }

    public static String normaliseColumnLabel(String name) {
        return name.toUpperCase();
    }

    private Integer columnPosition(String columnName) {
        return labelToColumn.get(normaliseColumnLabel(columnName));
    }

    private int columnIndex(int position) {
        return position - 1;
    }

}
