package com.asprotunity.tersql.internal;

import com.asprotunity.tersql.connection.Row;
import com.asprotunity.tersql.exception.RuntimeSQLException;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;

public class RowFromResultSet implements Row {

    private Object columns[];
    private HashMap<String, Integer> nameToColumn;

    public RowFromResultSet(ResultSet rs) {
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
    public Short asShort(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Short::valueOf, () -> ((Number) object)::shortValue);
    }

    @Override
    public Integer asInteger(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Integer::valueOf, () -> ((Number) object)::intValue);
    }

    @Override
    public Long asLong(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Long::valueOf, () -> ((Number) object)::longValue);
    }

    @Override
    public Double asDouble(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Double::valueOf, () -> ((Number) object)::doubleValue);
    }

    @Override
    public Float asFloat(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Float::valueOf, () -> ((Number) object)::floatValue);
    }

    @Override
    public Byte asByte(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Byte::valueOf, () -> ((Number) object)::byteValue);
    }

    @Override
    public BigDecimal asBigDecimal(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.toBigDecimal(object);
    }

    @Override
    public Boolean asBoolean(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.toBoolean(object);
    }

    @Override
    public Date asDate(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.toSqlDate(object);
    }

    @Override
    public Time asTime(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.toSqlTime(object);
    }

    @Override
    public Timestamp asTimestamp(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.toSqlTimestamp(object);
    }

    public String normaliseColumnName(String name) {
        return name.toUpperCase();
    }

    public Integer positionForColumn(String columnName) {
        return nameToColumn.get(normaliseColumnName(columnName));
    }

}
