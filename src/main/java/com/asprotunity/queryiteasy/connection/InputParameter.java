package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.disposer.Disposer;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Supplier;

@FunctionalInterface
public interface InputParameter {

    void accept(PreparedStatement statement, int position, Disposer disposer);

    static InputParameter bind(String value) {
        return (statement, position, disposer) -> RuntimeSQLException.execute(() ->
                statement.setString(position, value));
    }

    static InputParameter bind(Short value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.SMALLINT);
    }

    static InputParameter bind(Integer value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.INTEGER);
    }

    static InputParameter bind(Long value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.BIGINT);
    }

    static InputParameter bind(Double value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.DOUBLE);
    }

    static InputParameter bind(Float value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.REAL);
    }

    static InputParameter bind(Byte value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.TINYINT);
    }

    static InputParameter bind(Boolean value) {
        return (statement, position, disposer) ->
                setValue(value, statement, position, Types.BOOLEAN);
    }

    static InputParameter bind(BigDecimal value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setBigDecimal(position, value));
    }

    static InputParameter bind(Date value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setDate(position, value));
    }

    static InputParameter bind(Time value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setTime(position, value));
    }

    static InputParameter bind(Timestamp value) {
        return (statement, position, disposer) ->
                RuntimeSQLException.execute(() -> statement.setTimestamp(position, value));
    }

    static InputParameter bind(Supplier<InputStream> streamSupplier) {
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

    static void setValue(Object value, PreparedStatement statement, int position, int sqlType) {
        RuntimeSQLException.execute(() -> statement.setObject(position, value, sqlType));
    }
}
