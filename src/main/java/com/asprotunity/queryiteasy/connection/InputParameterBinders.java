package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Supplier;

public class InputParameterBinders {


    /**
     * Creates a new {@code InputParameter instance} to be used to store a string; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link String} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(String value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setString(position, value));
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a short; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Short} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Short value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.SMALLINT);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store an integer; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Integer} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Integer value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.INTEGER);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a long; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Long} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Long value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.BIGINT);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a double; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Double} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Double value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.DOUBLE);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a float; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Float} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Float value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.REAL);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a byte; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Byte} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Byte value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.TINYINT);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a byte array; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The byte array to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(byte[] value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setBytes(position, value));
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a {@code Boolean}; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link Boolean} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Boolean value) {
        return (statement, position, queryScope) ->
                setValue(statement, position, value, Types.BOOLEAN);
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a {@link java.math.BigDecimal}; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link java.math.BigDecimal} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(BigDecimal value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setBigDecimal(position, value));
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a {@link java.sql.Date}; a null instance
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link java.sql.Date} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Date value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setDate(position, value));
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a {@link java.sql.Time}; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link java.sql.Time} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Time value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setTime(position, value));
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to store a {@link java.sql.Timestamp}; a null value
     * will set the corresponding column to {@code NULL};
     *
     * @param value The {@link java.sql.Timestamp} to store. It can be null.
     * @return A new {@link InputParameter} instance.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
    public static InputParameter bind(Timestamp value) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setTimestamp(position, value));
    }

    /**
     * Creates a new {@code InputParameter instance} to be used to fill a blob in the database;
     * to set the blob to null make the supplier return a null value, e.g., {@code () -> null}.
     *
     * @param streamSupplier Provides the inputStream to use to fill the blob; to set the blob to null make the supplier
     *                       return a null value, e.g., {@code () -> null}.
     * @return A new {@link InputParameter} instance.
     * @throws InvalidArgumentException if {@code streamSupplier == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
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

    /**
     * Creates a new {@code InputParameter instance} to be used to fill a clob in the database;
     * to set the clob to null make the supplier return a null value, e.g., {@code () -> null}.
     *
     * @param readerSupplier Provides the reader to use to fill the clob; to set the blob to clob make the supplier
     *                       return a null value, e.g., {@code () -> null}.
     * @return A new {@link InputParameter} instance.
     * @throws InvalidArgumentException if {@code readerSupplier == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
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

    /**
     * Creates a new {@code InputParameter instance} to be used to fill a long varbinary in the database;
     * to set the long varbinary to null make the supplier return a null value, e.g., {@code () -> null}.
     *
     * @param streamSupplier Provides the inputStream to use to fill the long varbinary;
     *                       to set the long varbinary to null make the supplier return a null value, e.g., {@code () -> null}.
     * @return A new {@link InputParameter} instance.
     * @throws InvalidArgumentException if {@code streamSupplier == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     */
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
