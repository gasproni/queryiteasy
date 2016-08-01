package com.asprotunity.queryiteasy.connection;


import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.io.StreamIO.fromInputStream;
import static com.asprotunity.queryiteasy.io.StreamIO.fromReader;

/**
 * Groups all the default functions that can be used the {@link ResultSet} instances passed to the rowMapper lambda in
 * the <code>select</code> and <code>call</code> methods of the {@link Connection} interface.
 */
public abstract class ResultSetReaders {

    /**
     * Returns the number of columns in each {@code resultSet}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @return The number of columns inside the {@code resultSet} parameter.
     */
    public static int columnCount(ResultSet resultSet) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getMetaData().getColumnCount());
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getBinaryStream(columnIndex))};
     * if the stream is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @param streamReader A function that reads the content of the {@link InputStream}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getBinaryStream(columnIndex).
     */
    public static <ResultType> ResultType fromBinaryStream(ResultSet resultSet,
                                                           int columnIndex,
                                                           Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getBinaryStream(columnIndex), streamReader));
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getBinaryStream(columnLabel))};
     * if the stream is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @param streamReader A function that reads the content of the {@link InputStream}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getBinaryStream(columnLabel).
     */
    public static <ResultType> ResultType fromBinaryStream(ResultSet resultSet,
                                                           String columnLabel,
                                                           Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getBinaryStream(columnLabel), streamReader));
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getAsciiStream(columnIndex))};
     * if the stream is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @param streamReader A function that reads the content of the {@link InputStream}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getAsciiStream(columnIndex).
     */
    public static <ResultType> ResultType fromAsciiStream(ResultSet resultSet,
                                                          int columnIndex,
                                                          Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getAsciiStream(columnIndex), streamReader));
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getAsciiStream(columnLabel))};
     * if the stream is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @param streamReader A function that reads the content of the {@link InputStream}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getAsciiStream(columnLabel).
     */
    public static <ResultType> ResultType fromAsciiStream(ResultSet resultSet,
                                                          String columnLabel,
                                                          Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getAsciiStream(columnLabel), streamReader));
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getCharacterStream(columnIndex))};
     * if the reader is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @param streamReader A function that reads from the {@link Reader}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getCharacterStream(columnIndex).
     */
    public static <ResultType> ResultType fromCharacterStream(ResultSet resultSet,
                                                              int columnIndex,
                                                              Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getCharacterStream(columnIndex), streamReader));
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getCharacterStream(columnLabel))};
     * if the reader is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @param streamReader A function that reads from the {@link Reader}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getCharacterStream(columnLabel).
     */
    public static <ResultType> ResultType fromCharacterStream(ResultSet resultSet,
                                                              String columnLabel,
                                                              Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getCharacterStream(columnLabel), streamReader));
    }

    /**
     * Returns the result of  {@code streamReader.apply(resultSet.getNCharacterStream(columnIndex))};
     * if the reader is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @param streamReader A function that reads from the {@link Reader}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getNCharacterStream(columnIndex).
     */
    public static <ResultType> ResultType fromNCharacterStream(ResultSet resultSet,
                                                               int columnIndex,
                                                               Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getNCharacterStream(columnIndex), streamReader));
    }

    /**
     * Returns the result of {@code streamReader.apply(resultSet.getNCharacterStream(columnLabel))};
     * if the reader is not null, closes it after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @param streamReader A function that reads from the {@link Reader}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return The result of applying {@code streamReader} to {@code resultSet}.getNCharacterStream(columnLabel).
     */
    public static <ResultType> ResultType fromNCharacterStream(ResultSet resultSet,
                                                               String columnLabel,
                                                               Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getNCharacterStream(columnLabel), streamReader));
    }

    /**
     * If {@code resultSet.getBlob(columnIndex) == null} then returns the result of {@code streamReader.apply(null)};
     * otherwise returns the result of {@code streamReader.apply(resultSet.getBlob(columnIndex).getBinaryStream())},
     * and closes the binary stream and frees the blob after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @param streamReader A function that reads from the {@link InputStream}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return If {@code resultSet.getBlob(columnIndex) == null} then the result of {@code streamReader.apply(null)};
     *         the result of {@code streamReader.apply(resultSet.getBlob(columnIndex).getBinaryStream())} otherwise.
     */
    public static <ResultType> ResultType fromBlob(ResultSet resultSet,
                                                   int columnIndex,
                                                   Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromBlob(resultSet.getBlob(columnIndex), streamReader));
    }

    /**
     * If {@code resultSet.getBlob(columnLabel) == null} then returns the result of {@code streamReader.apply(null)};
     * otherwise returns the result of {@code streamReader.apply(resultSet.getBlob(columnLabel).getBinaryStream())},
     * and closes the binary stream and frees the blob after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @param streamReader A function that reads from the {@link InputStream}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return If {@code resultSet.getBlob(columnLabel) == null} then the result of {@code streamReader.apply(null)};
     *         the result of {@code streamReader.apply(resultSet.getBlob(columnLabel).getBinaryStream())} otherwise.
     */
    public static <ResultType> ResultType fromBlob(ResultSet resultSet,
                                                   String columnLabel,
                                                   Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromBlob(resultSet.getBlob(columnLabel), streamReader));
    }

    /**
     * If {@code resultSet.getClob(columnIndex) == null} then returns the result of {@code streamReader.apply(null)};
     * otherwise returns the result of {@code streamReader.apply(resultSet.getClob(columnIndex).getCharacterStream())},
     * and closes the character stream and frees the clob after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @param streamReader A function that reads from the {@link Reader}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return If {@code resultSet.getClob(columnIndex) == null} then the result of {@code streamReader.apply(null)};
     *         the result of {@code streamReader.apply(resultSet.getClob(columnIndex).getCharacterStream())} otherwise.
     */
    public static <ResultType> ResultType fromClob(ResultSet resultSet,
                                                   int columnIndex,
                                                   Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromClob(resultSet.getClob(columnIndex), streamReader));
    }

    /**
     * If {@code resultSet.getClob(columnLabel) == null} then returns the result of {@code streamReader.apply(null)};
     * otherwise returns the result of {@code streamReader.apply(resultSet.getClob(columnLabel).getCharacterStream())},
     * and closes the character stream and frees the clob after the call.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @param streamReader A function that reads from the {@link Reader}
     *                     into a {@code ResultType} instance.
     * @param <ResultType> A type supplied by the caller.
     * @throws InvalidArgumentException if {@code streamReader == null}.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return If {@code resultSet.getClob(columnLabel) == null} then the result of {@code streamReader.apply(null)};
     *         the result of {@code streamReader.apply(resultSet.getClob(columnLabel).getCharacterStream())} otherwise.
     */
    public static <ResultType> ResultType fromClob(ResultSet resultSet,
                                                   String columnLabel,
                                                   Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromClob(resultSet.getClob(columnLabel), streamReader));
    }

    /**
     * Returns the result of {@code resultSet.getBytes(columnIndex)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getBytes(columnIndex)}.
     */
    public static byte[] asByteArray(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBytes(columnIndex));
    }

    /**
     * Returns the result of {@code resultSet.getBytes(columnLabel)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getBytes(columnLabel)}.
     */
    public static byte[] asByteArray(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBytes(columnLabel));
    }

    /**
     * Returns the result of {@code resultSet.getString(columnLabel)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getString(columnLabel)}.
     */
    public static String asString(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getString(columnLabel));
    }

    /**
     * Returns the result of {@code resultSet.getString(columnIndex)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getString(columnIndex)}.
     */
    public static String asString(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getString(columnIndex));
    }

    /**
     * Returns the result of {@code resultSet.getShort(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getShort(columnLabel)}, or null if the value is null inside the database.
     */
    public static Short asShort(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getShort));
    }

    /**
     * Returns the result of {@code resultSet.getShort(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getShort(columnIndex)}, or null if the value is null inside the database.
     */
    public static Short asShort(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getShort));
    }

    /**
     * Returns the result of {@code resultSet.getInt(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getInt(columnLabel)}, or null if the value is null inside the database.
     */
    public static Integer asInteger(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getInt));
    }

    /**
     * Returns the result of {@code resultSet.getInt(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getInt(columnIndex)}, or null if the value is null inside the database.
     */
    public static Integer asInteger(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getInt));
    }

    /**
     * Returns the result of {@code resultSet.getLong(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getLong(columnLabel)}, or null if the value is null inside the database.
     */
    public static Long asLong(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getLong));
    }

    /**
     * Returns the result of {@code resultSet.getLong(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getLong(columnIndex)}, or null if the value is null inside the database.
     */
    public static Long asLong(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getLong));
    }

    /**
     * Returns the result of {@code resultSet.getDouble(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getDouble(columnIndex)}, or null if the value is null inside the database.
     */
    public static Double asDouble(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getDouble));
    }

    /**
     * Returns the result of {@code resultSet.getDouble(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getDouble(columnLabel)}, or null if the value is null inside the database.
     */
    public static Double asDouble(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getDouble));
    }

    /**
     * Returns the result of {@code resultSet.getFloat(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getFloat(columnLabel)}, or null if the value is null inside the database.
     */
    public static Float asFloat(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getFloat));
    }

    /**
     * Returns the result of {@code resultSet.getFloat(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getFloat(columnIndex)}, or null if the value is null inside the database.
     */
    public static Float asFloat(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getFloat));
    }

    /**
     * Returns the result of {@code resultSet.getByte(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getByte(columnLabel)}, or null if the value is null inside the database.
     */
    public static Byte asByte(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getByte));
    }

    /**
     * Returns the result of {@code resultSet.getByte(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getByte(columnIndex)}, or null if the value is null inside the database.
     */
    public static Byte asByte(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getByte));
    }

    /**
     * Returns the result of {@code resultSet.getBigDecimal(columnLabel)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getBigDecimal(columnLabel)}.
     */
    public static BigDecimal asBigDecimal(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBigDecimal(columnLabel));
    }

    /**
     * Returns the result of {@code resultSet.getBigDecimal(columnIndex)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getBigDecimal(columnIndex)}.
     */
    public static BigDecimal asBigDecimal(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBigDecimal(columnIndex));
    }

    /**
     * Returns the result of {@code resultSet.getBoolean(columnLabel)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getBoolean(columnLabel)}, or null if the value is null inside the database.
     */
    public static Boolean asBoolean(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getBoolean));
    }

    /**
     * Returns the result of {@code resultSet.getBoolean(columnIndex)}, or {@code null} if the value is null
     * inside the database.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getBoolean(columnIndex)}, or null if the value is null inside the database.
     */
    public static Boolean asBoolean(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLResultReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getBoolean));
    }

    /**
     * Returns the result of {@code resultSet.getDate(columnLabel)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getDate(columnLabel)}.
     */
    public static Date asDate(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getDate(columnLabel));
    }

    /**
     * Returns the result of {@code resultSet.getDate(columnIndex)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getDate(columnIndex)}.
     */
    public static Date asDate(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getDate(columnIndex));
    }

    /**
     * Returns the result of {@code resultSet.getTime(columnLabel)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getTime(columnLabel)}.
     */
    public static Time asTime(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTime(columnLabel));
    }

    /**
     * Returns the result of {@code resultSet.getTime(columnIndex)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getTime(columnIndex)}.
     */
    public static Time asTime(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTime(columnIndex));
    }

    /**
     * Returns the result of {@code resultSet.getTimestamp(columnLabel)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnLabel The label of the column to read.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getTimestamp(columnLabel)}.
     */
    public static Timestamp asTimestamp(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTimestamp(columnLabel));
    }

    /**
     * Returns the result of {@code resultSet.getTimestamp(columnIndex)}.
     *
     * @param resultSet The {@link java.sql.ResultSet} wrapping the results of a query.
     * @param columnIndex The index of the column to read. Starts at 1.
     * @throws RuntimeSQLException if a {@link java.sql.SQLException} is thrown during the call.
     * @return {@code resultSet.getTimestamp(columnIndex)}.
     */
    public static Timestamp asTimestamp(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTimestamp(columnIndex));
    }

}
