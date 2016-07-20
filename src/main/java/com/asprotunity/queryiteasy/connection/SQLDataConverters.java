package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.AutoCloseableScope;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.function.Function;

public final class SQLDataConverters {

    public static <ResultType> ResultType fromBlob(Blob blob,
                                                   Function<InputStream, ResultType> blobReader) {
        if (blob == null) {
            return blobReader.apply(null);
        } else {
            try (AutoCloseableScope scope = new AutoCloseableScope();
                 InputStream inputStream = blob.getBinaryStream()) {
                scope.add(blob::free);
                return blobReader.apply(inputStream);
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    public static <ResultType> ResultType fromClob(Clob clob,
                                                   Function<Reader, ResultType> clobReader) {
        if (clob == null) {
            return clobReader.apply(null);
        } else {
            try (AutoCloseableScope scope = new AutoCloseableScope();
                 Reader reader = clob.getCharacterStream()) {
                scope.add(clob::free);
                return clobReader.apply(reader);
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    public static <ResultType> ResultType fromInputStream(InputStream inputStream,
                                                          Function<InputStream, ResultType> inputStreamReader) {
        if (inputStream == null) {
            return null;
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(inputStream::close);
            return inputStreamReader.apply(inputStream);
        }
    }

    public static <ResultType> ResultType fromReader(Reader reader,
                                                     Function<Reader, ResultType> inputStreamReader) {
        if (reader == null) {
            return null;
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(reader::close);
            return inputStreamReader.apply(reader);
        }
    }

    public static Short asShort(Object value) {
        return convertNumber(value,
                Short.class, Short::valueOf, Number::shortValue);
    }

    public static Integer asInteger(Object value) {
        return convertNumber(value,
                Integer.class, Integer::valueOf, Number::intValue);
    }

    public static Long asLong(Object value) {
        return convertNumber(value,
                Long.class, Long::valueOf, Number::longValue);
    }

    public static Double asDouble(Object value) {
        return convertNumber(value,
                Double.class, Double::valueOf, Number::doubleValue);
    }

    public static Float asFloat(Object value) {
        return convertNumber(value,
                Float.class, Float::valueOf, Number::floatValue);
    }

    public static Byte asByte(Object value) {
        return convertNumber(value,
                Byte.class, Byte::valueOf, Number::byteValue);
    }

    public static Time asTime(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Time) {
            return (Time) value;
        } else if (value instanceof String) {
            return Time.valueOf((String) value);
        } else if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            return new Time(timestamp.getTime());
        }
        throw new ClassCastException(classCastExceptionMessage(value, Time.class));
    }

    public static Timestamp asTimestamp(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Timestamp) {
            return (Timestamp) value;
        } else if (value instanceof java.util.Date) {
            return Timestamp.from(((java.util.Date) value).toInstant());
        } else if (value instanceof String) {
            return Timestamp.valueOf((String) value);
        }
        throw new ClassCastException(classCastExceptionMessage(value, Timestamp.class));
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
}
