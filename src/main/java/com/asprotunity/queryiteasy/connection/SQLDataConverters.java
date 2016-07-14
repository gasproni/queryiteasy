package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.AutoCloseableScope;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Function;

public final class SQLDataConverters {

    public static <ResultType> ResultType fromBlob(Object object,
                                                   Function<InputStream, ResultType> blobReader) {
        if (object == null) {
            return blobReader.apply(null);
        } else if (object instanceof Blob) {
            Blob blob = (Blob) object;
            try (AutoCloseableScope scope = new AutoCloseableScope();
                 InputStream inputStream = blob.getBinaryStream()) {
                scope.add(blob::free);
                return blobReader.apply(inputStream);
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else if (object instanceof byte[]) {
            // This is for MySQL. Sometimes it returns blobs as byte[].
            try (InputStream inputStream = new ByteArrayInputStream((byte[]) object)) {
                return blobReader.apply(inputStream);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else {
            throw new ClassCastException("Cannot extract blob data from " + object.getClass());
        }
    }

    public static <ResultType> ResultType fromClob(Object object,
                                                   Function<Reader, ResultType> clobReader) {
        if (object == null) {
            return clobReader.apply(null);
        } else if (object instanceof Clob) {
            Clob clob = (Clob) object;
            try (AutoCloseableScope scope = new AutoCloseableScope();
                 Reader reader = clob.getCharacterStream()) {
                scope.add(clob::free);
                return clobReader.apply(reader);
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else if (object instanceof String) {
            // This is for MySQL 'TEXT' SQL type.
            try (Reader reader = new StringReader((String) object)) {
                return clobReader.apply(reader);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else {
            throw new ClassCastException("Cannot extract clob data from " + object.getClass());
        }
    }

    public static <ResultType> ResultType fromLongVarbinary(Object object,
                                                            Function<InputStream, ResultType> longvarbinaryReader) {
        return fromBlob(object, longvarbinaryReader);
    }

    public static <ResultType> ResultType fromBinaryStream(InputStream inputStream,
                                                           Function<InputStream, ResultType> binaryReader) {
        if (inputStream == null) {
            return null;
        }
        try (AutoCloseableScope scope = new AutoCloseableScope()) {
            scope.add(inputStream::close);
            return binaryReader.apply(inputStream);
        }
    }

    public static String asString(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
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

    public static BigDecimal asBigDecimal(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof String) {
            return new BigDecimal((String) value);
        } else if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        throw new ClassCastException(classCastExceptionMessage(value, BigDecimal.class));
    }

    public static Boolean asBoolean(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.valueOf((String) value);
        }
        throw new ClassCastException(classCastExceptionMessage(value, Boolean.class));
    }

    public static Date asDate(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof String) {
            return Date.valueOf((String) value);
        } else if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            return new Date(timestamp.getTime());
        }
        throw new ClassCastException(classCastExceptionMessage(value, Date.class));
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
