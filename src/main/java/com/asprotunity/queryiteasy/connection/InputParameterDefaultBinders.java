package com.asprotunity.queryiteasy.connection;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Supplier;

public class InputParameterDefaultBinders {


    public static InputParameter bind(String value) {
        return (statement, position, disposer) -> RuntimeSQLException.execute(() ->
                statement.setString(position, value));
    }

    public static InputParameter bind(Short value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.SMALLINT);
    }

    public static InputParameter bind(Integer value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.INTEGER);
    }

    public static InputParameter bind(Long value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.BIGINT);
    }

    public static InputParameter bind(Double value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.DOUBLE);
    }

    public static InputParameter bind(Float value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.REAL);
    }

    public static InputParameter bind(Byte value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.TINYINT);
    }

    public static InputParameter bind(Boolean value) {
        return (statement, position, disposer) ->
                setValue(statement, position, value, Types.BOOLEAN);
    }

    public static InputParameter bind(BigDecimal value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setBigDecimal(position, value));
    }

    public static InputParameter bind(Date value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setDate(position, value));
    }

    public static InputParameter bind(Time value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setTime(position, value));
    }

    public static InputParameter bind(Timestamp value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setTimestamp(position, value));
    }

    public static InputParameter bind(Supplier<InputStream> streamSupplier) {
        return (statement, position, disposer) -> {
            InputStream inputStream = streamSupplier.get();
            RuntimeSQLException.execute(() -> {
                if (inputStream == null) {
                    statement.setNull(position, Types.BLOB);
                } else {
                    disposer.onClose(inputStream::close);
                    statement.setBlob(position, inputStream);
                }
            });
        };
    }

    private static void setValue(PreparedStatement statement, int position, Object value, int sqlType) {
        RuntimeSQLException.execute(() -> statement.setObject(position, value, sqlType));
    }
}
