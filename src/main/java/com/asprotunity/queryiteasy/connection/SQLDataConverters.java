package com.asprotunity.queryiteasy.connection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Optional;
import java.util.function.Function;

public final class SQLDataConverters {

    public static <T extends Number> T convertNumber(Object object, Class<T> targetType, Function<String, T> valueOf, Function<Number, T>
            toValueInstanceMethod) {
        if (object == null) {
            return null;
        } else if (object.getClass().equals(targetType)) {
            @SuppressWarnings("unchecked")
            T result = (T) object;
            return result;
        } else if (object instanceof Number) {
            return toValueInstanceMethod.apply((Number) object);
        } else if (object instanceof String) {
            return valueOf.apply((String) object);
        }
        throw new ClassCastException(classCastExceptionMessage(object, targetType));
    }

    private static <T> String classCastExceptionMessage(Object object, Class<T> targetType) {
        return object.getClass().getCanonicalName() + " cannot be cast to " +
                targetType.getCanonicalName();
    }

    public static <ResultType> ResultType fromBlob(Object object,
                                                   Function<Optional<InputStream>, ResultType> blobReader) {
        if (object == null) {
            return blobReader.apply(Optional.empty());
        } else if (object instanceof Blob) {
            Blob blob = (Blob) object;
            try (InputStream inputStream = blob.getBinaryStream()) {
                return blobReader.apply(Optional.of(inputStream));
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else if (object instanceof byte[]) {
            try (InputStream inputStream = new ByteArrayInputStream((byte[]) object)) {
                return blobReader.apply(Optional.of(inputStream));
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else {
            throw new ClassCastException("Cannot extract blob data from " + object.getClass());
        }
    }

    public static String asString(Object column) {
        return String.valueOf(column);
    }

    public static Short asShort(Object column) {
        return convertNumber(column,
                Short.class, Short::valueOf, Number::shortValue);
    }

    public static Integer asInteger(Object column) {
        return convertNumber(column,
                Integer.class, Integer::valueOf, Number::intValue);
    }

    public static Long asLong(Object column) {
        return convertNumber(column,
                Long.class, Long::valueOf, Number::longValue);
    }

    public static Double asDouble(Object column) {
        return convertNumber(column,
                Double.class, Double::valueOf, Number::doubleValue);
    }

    public static Float asFloat(Object column) {
        return convertNumber(column,
                Float.class, Float::valueOf, Number::floatValue);
    }

    public static Byte asByte(Object column) {
        return convertNumber(column,
                Byte.class, Byte::valueOf, Number::byteValue);
    }

    public static BigDecimal asBigDecimal(Object column) {
        if (column == null) {
            return null;
        } else if (column instanceof BigDecimal) {
            return (BigDecimal) column;
        } else if (column instanceof String) {
            return new BigDecimal((String) column);
        } else if (column instanceof Number) {
            return BigDecimal.valueOf(((Number) column).doubleValue());
        }
        throw new ClassCastException(classCastExceptionMessage(column, BigDecimal.class));
    }

    public static Boolean asBoolean(Object column) {
        if (column == null) {
            return null;
        } else if (column instanceof Boolean) {
            return (Boolean) column;
        } else if (column instanceof String) {
            return Boolean.valueOf((String) column);
        }
        throw new ClassCastException(classCastExceptionMessage(column, Boolean.class));
    }

    public static Date asDate(Object column) {
        if (column == null) {
            return null;
        } else if (column instanceof Date) {
            return (Date) column;
        } else if (column instanceof String) {
            return Date.valueOf((String) column);
        } else if (column instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) column;
            return new Date(timestamp.getTime());
        }
        throw new ClassCastException(classCastExceptionMessage(column, Date.class));
    }

    public static Time asTime(Object column) {
        if (column == null) {
            return null;
        } else if (column instanceof Time) {
            return (Time) column;
        } else if (column instanceof String) {
            return Time.valueOf((String) column);
        } else if (column instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) column;
            return new Time(timestamp.getTime());
        }
        throw new ClassCastException(classCastExceptionMessage(column, Time.class));
    }

    public static Timestamp asTimestamp(Object column) {
        if (column == null) {
            return null;
        } else if (column instanceof Timestamp) {
            return (Timestamp) column;
        } else if (column instanceof java.util.Date) {
            return Timestamp.from(((java.util.Date) column).toInstant());
        } else if (column instanceof String) {
            return Timestamp.valueOf((String) column);
        }
        throw new ClassCastException(classCastExceptionMessage(column, Timestamp.class));
    }
}
