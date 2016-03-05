package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.connection.RuntimeIOException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Optional;
import java.util.function.Function;

public final class TypeConverters {

    public static BigDecimal toBigDecimal(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof BigDecimal) {
            return (BigDecimal) object;
        } else if (object instanceof String) {
            return new BigDecimal((String) object);
        } else if (object instanceof Number) {
            return BigDecimal.valueOf(((Number) object).doubleValue());
        }
        throw new ClassCastException(classCastExceptionMessage(object, BigDecimal.class));
    }

    public static Boolean toBoolean(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Boolean) {
            return (Boolean) object;
        } else if (object instanceof String) {
            return Boolean.valueOf((String) object);
        }
        throw new ClassCastException(classCastExceptionMessage(object, Boolean.class));
    }

    public static Date toSqlDate(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Date) {
            return (Date) object;
        } else if (object instanceof String) {
            return Date.valueOf((String) object);
        } else if (object instanceof Timestamp) {
            Timestamp timestamp = (Timestamp)object;
            return new java.sql.Date(timestamp.getTime());
        }
        throw new ClassCastException(classCastExceptionMessage(object, Date.class));
    }

    public static Time toSqlTime(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Time) {
            return (Time) object;
        } else if (object instanceof String) {
            return Time.valueOf((String) object);
        } else if (object instanceof Timestamp) {
            Timestamp timestamp = (Timestamp)object;
            return new java.sql.Time(timestamp.getTime());
        }
        throw new ClassCastException(classCastExceptionMessage(object, Time.class));
    }

    public static Timestamp toSqlTimestamp(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Timestamp) {
            return (Timestamp) object;
        } else if (object instanceof java.util.Date) {
            return Timestamp.from(((java.util.Date) object).toInstant());
        } else if (object instanceof String) {
            return Timestamp.valueOf((String) object);
        }
        throw new ClassCastException(classCastExceptionMessage(object, Timestamp.class));
    }

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
}
