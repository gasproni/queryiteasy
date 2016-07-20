package com.asprotunity.queryiteasy.acceptance_tests;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public class FlexibleTupleFromResultSet implements FlexibleTuple {

    private Object columns[];
    private HashMap<String, Integer> labelToColumn;

    public FlexibleTupleFromResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        columns = new Object[metaData.getColumnCount()];
        labelToColumn = new HashMap<>();
        for (int columnPosition = 1; columnPosition <= columns.length; ++columnPosition) {
            columns[columnIndex(columnPosition)] = resultSet.getObject(columnPosition);
            labelToColumn.put(normaliseColumnLabel(metaData.getColumnLabel(columnPosition)), columnPosition);
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

    private static String normaliseColumnLabel(String name) {
        return name.toUpperCase();
    }

    private Integer columnPosition(String columnName) {
        return labelToColumn.get(normaliseColumnLabel(columnName));
    }

    private int columnIndex(int position) {
        return position - 1;
    }

}
