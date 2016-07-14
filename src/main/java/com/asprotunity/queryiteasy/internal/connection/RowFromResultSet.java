package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;

import java.util.HashMap;

public class RowFromResultSet implements Row {

    private Object columns[];
    private HashMap<String, Integer> labelToColumn;

    public RowFromResultSet(ResultSetWrapper resultSetWrapper) {
        columns = new Object[resultSetWrapper.columnCount()];
        labelToColumn = new HashMap<>();
        for (int columnIndex = 1; columnIndex <= columns.length; ++columnIndex) {
            int position = columnIndex(columnIndex);
            Object object = resultSetWrapper.getObject(columnIndex);
            columns[position] = object;
            labelToColumn.put(normaliseColumnLabel(resultSetWrapper.columnLabel(columnIndex)), columnIndex);
        }
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
