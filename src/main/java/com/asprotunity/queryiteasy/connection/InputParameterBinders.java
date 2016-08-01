package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Supplier;

public class InputParameterBinders {


    public static InputParameter bind(String value) {
        return (statement, position, queryScope) -> RuntimeSQLException.execute(() ->
                                                                                        statement.setString(position, value));
    }

    public static InputParameter bind(Short value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.SMALLINT);
    }

    public static InputParameter bind(Integer value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.INTEGER);
    }

    public static InputParameter bind(Long value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.BIGINT);
    }

    public static InputParameter bind(Double value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.DOUBLE);
    }

    public static InputParameter bind(Float value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.REAL);
    }

    public static InputParameter bind(Byte value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.TINYINT);
    }

    public static InputParameter bind(byte[] value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setBytes(position, value));
    }

    public static InputParameter bind(Boolean value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.BOOLEAN);
    }

    public static InputParameter bind(BigDecimal value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setBigDecimal(position, value));
    }

    public static InputParameter bind(Date value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setDate(position, value));
    }

    public static InputParameter bind(Time value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setTime(position, value));
    }

    public static InputParameter bind(Timestamp value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setTimestamp(position, value));
    }

    public static InputParameter bindBlob(Supplier<InputStream> streamSupplier) {
        InvalidArgumentException.throwIfNull(streamSupplier, "streamSupplier");
        return (statement, position, queryScope) -> {
            InputStream inputStream = streamSupplier.get();
            RuntimeSQLException.execute(() -> {
                if (inputStream == null) {
                    statement.setNull(position, Types.BLOB);
                } else {
                    queryScope.add(inputStream::close);
                    statement.setBlob(position, inputStream);
                }
            });
        };
    }

    public static InputParameter bindClob(Supplier<Reader> readerSupplier) {
        InvalidArgumentException.throwIfNull(readerSupplier, "readerSupplier");
        return (statement, position, queryScope) -> {
            Reader reader = readerSupplier.get();
            RuntimeSQLException.execute(() -> {
                if (reader == null) {
                    statement.setNull(position, Types.CLOB);
                } else {
                    queryScope.add(reader::close);
                    statement.setClob(position, reader);
                }
            });
        };
    }

    public static InputParameter bindLongVarbinary(Supplier<InputStream> streamSupplier) {
        InvalidArgumentException.throwIfNull(streamSupplier, "streamSupplier");
        return (statement, position, queryScope) -> {
            InputStream inputStream = streamSupplier.get();
            RuntimeSQLException.execute(() -> {
                if (inputStream == null) {
                    statement.setNull(position, Types.LONGVARBINARY);
                } else {
                    queryScope.add(inputStream::close);
                    statement.setBinaryStream(position, inputStream);
                }
            });
        };
    }

    private static void setValue(PreparedStatement statement, int position, Object value, int sqlType) {
        RuntimeSQLException.execute(() -> statement.setObject(position, value, sqlType));
    }
}
