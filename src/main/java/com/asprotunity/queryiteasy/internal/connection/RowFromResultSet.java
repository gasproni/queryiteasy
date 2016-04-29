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
    public String asString(String columnName) {
        return asString(columnPosition(columnName));
    }

    @Override
    public String asString(int position) {
        return String.valueOf(columns[columnIndex(position)]);
    }

    @Override
    public Short asShort(String columnName) {
        return asShort(columnPosition(columnName));
    }

    @Override
    public Short asShort(int position) {
        return TypeConverters.convertNumber(columns[columnIndex(position)],
                Short.class, Short::valueOf, Number::shortValue);
    }

    @Override
    public Integer asInteger(String columnName) {
        return asInteger(columnPosition(columnName));
    }

    @Override
    public Integer asInteger(int position) {
        return TypeConverters.convertNumber(columns[columnIndex(position)],
                Integer.class, Integer::valueOf, Number::intValue);
    }

    @Override
    public Long asLong(String columnName) {
        return asLong(columnPosition(columnName));
    }

    @Override
    public Long asLong(int position) {
        return TypeConverters.convertNumber(columns[columnIndex(position)],
                Long.class, Long::valueOf, Number::longValue);
    }

    @Override
    public Double asDouble(String columnName) {
        return asDouble(columnPosition(columnName));
    }

    @Override
    public Double asDouble(int position) {
        return TypeConverters.convertNumber(columns[columnIndex(position)],
                Double.class, Double::valueOf, Number::doubleValue);
    }

    @Override
    public Float asFloat(String columnName) {
        return asFloat(columnPosition(columnName));
    }

    @Override
    public Float asFloat(int position) {
        return TypeConverters.convertNumber(columns[columnIndex(position)],
                Float.class, Float::valueOf, Number::floatValue);
    }

    @Override
    public Byte asByte(String columnName) {
        return asByte(columnPosition(columnName));
    }

    @Override
    public Byte asByte(int position) {
        return TypeConverters.convertNumber(columns[columnIndex(position)],
                Byte.class, Byte::valueOf, Number::byteValue);
    }

    @Override
    public BigDecimal asBigDecimal(String columnName) {
        return asBigDecimal(columnPosition(columnName));
    }

    @Override
    public BigDecimal asBigDecimal(int position) {
        return TypeConverters.toBigDecimal(columns[columnIndex(position)]);
    }

    @Override
    public Boolean asBoolean(String columnName) {
        return asBoolean(columnPosition(columnName));
    }

    @Override
    public Boolean asBoolean(int position) {
        return TypeConverters.toBoolean(columns[columnIndex(position)]);
    }

    @Override
    public Date asDate(String columnName) {
        return asDate(columnPosition(columnName));
    }

    @Override
    public Date asDate(int position) {
        return TypeConverters.toSqlDate(columns[columnIndex(position)]);
    }

    @Override
    public Time asTime(String columnName) {
        return asTime(columnPosition(columnName));
    }

    @Override
    public Time asTime(int position) {
        return TypeConverters.toSqlTime(columns[columnIndex(position)]);
    }

    @Override
    public Timestamp asTimestamp(String columnName) {
        return asTimestamp(columnPosition(columnName));
    }

    @Override
    public Timestamp asTimestamp(int position) {
        return TypeConverters.toSqlTimestamp(columns[columnIndex(position)]);
    }

    @Override
    public <ResultType> ResultType fromBlob(String columnName,
                                            Function<Optional<InputStream>, ResultType> blobReader) {
        return fromBlob(columnPosition(columnName), blobReader);
    }

    @Override
    public <ResultType> ResultType fromBlob(int position,
                                            Function<Optional<InputStream>, ResultType> blobReader) {
        Object object = columns[columnIndex(position)];
        return TypeConverters.fromBlob(object, blobReader);
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
