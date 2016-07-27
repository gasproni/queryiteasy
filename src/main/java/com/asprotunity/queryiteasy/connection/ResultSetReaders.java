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

public abstract class ResultSetReaders {

    public static int columnCount(ResultSet resultSet) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getMetaData().getColumnCount());
    }

    public static <ResultType> ResultType fromBinaryStream(ResultSet resultSet,
                                                           int columnIndex,
                                                           Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getBinaryStream(columnIndex), streamReader));
    }

    public static <ResultType> ResultType fromBinaryStream(ResultSet resultSet,
                                                           String columnLabel,
                                                           Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getBinaryStream(columnLabel), streamReader));
    }


    public static <ResultType> ResultType fromAsciiStream(ResultSet resultSet,
                                                          int columnIndex,
                                                          Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getAsciiStream(columnIndex), streamReader));
    }

    public static <ResultType> ResultType fromAsciiStream(ResultSet resultSet,
                                                          String columnLabel,
                                                          Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getAsciiStream(columnLabel), streamReader));
    }

    public static <ResultType> ResultType fromCharacterStream(ResultSet resultSet,
                                                              int columnIndex,
                                                              Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getCharacterStream(columnIndex), streamReader));
    }

    public static <ResultType> ResultType fromCharacterStream(ResultSet resultSet,
                                                              String columnLabel,
                                                              Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getCharacterStream(columnLabel), streamReader));
    }

    public static <ResultType> ResultType fromNCharacterStream(ResultSet resultSet,
                                                               int columnIndex,
                                                               Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getNCharacterStream(columnIndex), streamReader));
    }

    public static <ResultType> ResultType fromNCharacterStream(ResultSet resultSet,
                                                               String columnLabel,
                                                               Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getNCharacterStream(columnLabel), streamReader));
    }

    public static <ResultType> ResultType fromBlob(ResultSet resultSet,
                                                   int columnIndex,
                                                   Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromBlob(resultSet.getBlob(columnIndex), streamReader));
    }

    public static <ResultType> ResultType fromBlob(ResultSet resultSet,
                                                   String columnLabel,
                                                   Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromBlob(resultSet.getBlob(columnLabel), streamReader));
    }

    public static <ResultType> ResultType fromClob(ResultSet resultSet,
                                                   int columnIndex,
                                                   Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromClob(resultSet.getClob(columnIndex), streamReader));
    }

    public static <ResultType> ResultType fromClob(ResultSet resultSet,
                                                   String columnLabel,
                                                   Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromClob(resultSet.getClob(columnLabel), streamReader));
    }

    public static byte[] asByteArray(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBytes(columnIndex));
    }

    public static byte[] asByteArray(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBytes(columnLabel));
    }

    public static String asString(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getString(columnLabel));
    }

    public static String asString(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getString(columnIndex));
    }

    public static Short asShort(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getShort));
    }

    public static Short asShort(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getShort));
    }

    public static Integer asInteger(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getInt));
    }

    public static Integer asInteger(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getInt));
    }

    public static Long asLong(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getLong));
    }

    public static Long asLong(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getLong));
    }

    public static Double asDouble(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getDouble));
    }

    public static Double asDouble(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getDouble));
    }

    public static Float asFloat(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getFloat));
    }

    public static Float asFloat(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getFloat));
    }

    public static Byte asByte(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getByte));
    }

    public static Byte asByte(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getByte));
    }

    public static BigDecimal asBigDecimal(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBigDecimal(columnLabel));
    }

    public static BigDecimal asBigDecimal(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBigDecimal(columnIndex));
    }

    public static Boolean asBoolean(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnLabel, ResultSet::getBoolean));
    }

    public static Boolean asBoolean(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> SQLValueReaders.returnValueOrNull(resultSet, columnIndex, ResultSet::getBoolean));
    }

    public static Date asDate(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getDate(columnLabel));
    }

    public static Date asDate(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getDate(columnIndex));
    }

    public static Time asTime(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTime(columnLabel));
    }

    public static Time asTime(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTime(columnIndex));
    }

    public static Timestamp asTimestamp(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTimestamp(columnLabel));
    }

    public static Timestamp asTimestamp(ResultSet resultSet, int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTimestamp(columnIndex));
    }

}
