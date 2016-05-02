package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public class RowFromResultSet implements Row {

    private Object columns[];
    private HashMap<String, Integer> labelToColumn;

    public RowFromResultSet(ResultSet rs) {
        try {
            ResultSetMetaData metadata = rs.getMetaData();
            columns = new Object[metadata.getColumnCount()];
            labelToColumn = new HashMap<>();
            for (int columnIndex = 1; columnIndex <= columns.length; ++columnIndex) {
                int position = columnIndex(columnIndex);
                columns[position] = rs.getObject(columnIndex);
                labelToColumn.put(normaliseColumnLabel(metadata.getColumnLabel(columnIndex)), columnIndex);
            }

        } catch (SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    @Override
    public Object at(String columnName) {
        return at(columnPosition(columnName));
    }

    @Override
    public Object at(int columnPosition) {
        return columns[columnIndex(columnPosition)];
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
