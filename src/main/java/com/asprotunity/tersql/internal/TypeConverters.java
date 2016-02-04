package com.asprotunity.tersql.internal;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Function;
import java.util.function.Supplier;

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
            return (Boolean)object;
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

    public static <T extends Number> T convertNumber(Object object, Function<String, T> valueOf, Supplier<Supplier<T>> objectMethodCaller) {
        if (object == null) {
            return null;
        }
        else if (object instanceof Number) {
            return objectMethodCaller.get().get();
        }
        else if (object instanceof String) {
            return valueOf.apply((String) object);
        }
        throw new ClassCastException("Invalid cast:" + object.getClass().getName());
    }

    private static <T> String classCastExceptionMessage(Object object, Class<T> targetType) {
        return object.getClass().getCanonicalName() + " cannot be cast to " +
                targetType.getCanonicalName();
    }
}
