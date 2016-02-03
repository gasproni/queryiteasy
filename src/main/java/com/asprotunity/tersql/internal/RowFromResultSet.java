package com.asprotunity.tersql.internal;

import com.asprotunity.tersql.connection.Row;
import com.asprotunity.tersql.exception.RuntimeSQLException;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Supplier;

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
        return convertNumber(object, Short::valueOf, () -> ((Number) object)::shortValue);
    }

    @Override
    public Integer asInteger(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return convertNumber(object, Integer::valueOf, () -> ((Number) object)::intValue);
    }

    @Override
    public Long asLong(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return convertNumber(object, Long::valueOf, () -> ((Number) object)::longValue);
    }

    @Override
    public Double asDouble(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return convertNumber(object, Double::valueOf, () -> ((Number) object)::doubleValue);
    }

    @Override
    public Float asFloat(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return convertNumber(object, Float::valueOf, () -> ((Number) object)::floatValue);
    }

    @Override
    public Byte asByte(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        return convertNumber(object, Byte::valueOf, () -> ((Number) object)::byteValue);
    }

    @Override
    public BigDecimal asBigDecimal(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof BigDecimal) {
            return (BigDecimal) object;
        } else if (object instanceof String) {
            return new BigDecimal((String) object);
        } else if (object instanceof Number) {
            return BigDecimal.valueOf(((Number) object).doubleValue());
        }
        throw new ClassCastException("Invalid cast:" + object.getClass().getName());
    }

    @Override
    public Boolean asBoolean(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return Boolean.valueOf((String) object);
        } else {
            return (Boolean) object;
        }
    }

    @Override
    public Date asDate(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof Date) {
            return (Date) object;
        }
        else if (object instanceof String) {
            return Date.valueOf((String) object);
        }
        throw new ClassCastException("Invalid cast:" + object.getClass().getName());
    }

    @Override
    public Time asTime(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof Time) {
            return (Time) object;
        } else if (object instanceof String) {
            return Time.valueOf((String) object);
        }
        throw new ClassCastException("Invalid cast:" + object.getClass().getName());
    }

    @Override
    public Timestamp asTimestamp(String columnName) {
        Object object = columns[positionForColumn(columnName)];
        if (object == null) {
            return null;
        } else if (object instanceof Timestamp) {
            return (Timestamp) object;
        } else if (object instanceof java.util.Date) {
            return Timestamp.from(((java.util.Date)object).toInstant());
        } else if (object instanceof String) {
            return Timestamp.valueOf((String) object);
        }
        throw new ClassCastException("Invalid cast:" + object.getClass().getName());
    }

    public String normaliseColumnName(String name) {
        return name.toUpperCase();
    }

    public Integer positionForColumn(String columnName) {
        return nameToColumn.get(normaliseColumnName(columnName));
    }

    private <T extends Number> T convertNumber(Object object, Function<String, T> valueOf, Supplier<Supplier<T>> objectMethodCaller) {
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return valueOf.apply((String) object);
        } else if (object instanceof Number) {
            return objectMethodCaller.get().get();
        }
        throw new ClassCastException("Invalid cast:" + object.getClass().getName());
    }

}
