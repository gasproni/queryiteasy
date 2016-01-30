package com.asprotunity.tersql.internal;

import com.asprotunity.tersql.connection.Row;
import com.asprotunity.tersql.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public class WrappedResultSet implements Row {

    private Object columns[];
    private HashMap<String, Integer> nameToColumn;

    public WrappedResultSet(ResultSet rs) {
        try {
            ResultSetMetaData metadata = rs.getMetaData();
            columns = new Object[metadata.getColumnCount()];
            nameToColumn = new HashMap<>();
            for (int columnIndex = 1; columnIndex <= columns.length; ++columnIndex) {
                int position = columnIndex - 1;
                columns[position] = rs.getObject(columnIndex);
                nameToColumn.put(metadata.getColumnLabel(columnIndex), position);
            }

        } catch (SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    @Override
    public String asString(String columnName) {
        return String.valueOf(columns[positionForColumn(columnName)]);
    }

    @Override
    public Integer asInteger(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return Integer.valueOf((String) object);
        } else if (object instanceof Number) {
            return ((Number) object).intValue();
        }
        throw new RuntimeException("Invalid cast:" + object.getClass().getName());
    }

    @Override
    public Double asDouble(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return Double.valueOf((String) object);
        } else if (object instanceof Number) {
            return ((Number) object).doubleValue();
        }
        throw new RuntimeException("Invalid cast:" + object.getClass().getName());
    }

    @Override
    public Float asFloat(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return Float.valueOf((String) object);
        } else if (object instanceof Number) {
            return ((Number) object).floatValue();
        }
        throw new RuntimeException("Invalid cast:" + object.getClass().getName());
    }

    @Override
    public Byte asByte(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return Byte.valueOf((String) object);
        } else if (object instanceof Number) {
            return ((Number) object).byteValue();
        }
        throw new RuntimeException("Invalid cast:" + object.getClass().getName());
    }


    public String normaliseColumnName(String name) {
        return name.toUpperCase();
    }

    public Integer positionForColumn(String columnName) {
        return nameToColumn.get(normaliseColumnName(columnName));
    }

}
