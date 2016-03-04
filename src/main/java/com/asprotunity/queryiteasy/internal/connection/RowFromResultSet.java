package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;

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
                nameToColumn.put(normaliseColumnName(metadata.getColumnLabel(columnIndex)), position);
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
        return TypeConverters.convertNumber(object, Short.class, Short::valueOf, Number::shortValue);
    }

    @Override
    public Integer asInteger(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Integer.class, Integer::valueOf, Number::intValue);
    }

    @Override
    public Long asLong(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Long.class, Long::valueOf, Number::longValue);
    }

    @Override
    public Double asDouble(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Double.class, Double::valueOf, Number::doubleValue);
    }

    @Override
    public Float asFloat(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Float.class, Float::valueOf, Number::floatValue);
    }

    @Override
    public Byte asByte(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.convertNumber(object, Byte.class, Byte::valueOf, Number::byteValue);
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

    @Override
    public <ResultType> ResultType fromBlob(String columnName,
                                            Function<Optional<InputStream>, ResultType> blobReader) {
        Object object = columns[positionForColumn(columnName)];
        return TypeConverters.fromBlob(object, blobReader);
    }

    public static String normaliseColumnName(String name) {
        return name.toUpperCase();
    }

    public Integer positionForColumn(String columnName) {
        return nameToColumn.get(normaliseColumnName(columnName));
    }

}
