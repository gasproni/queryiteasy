package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.io.StreamIO.fromInputStream;
import static com.asprotunity.queryiteasy.io.StreamIO.fromReader;

public abstract class RowDefaults implements Row {
    private final ResultSet resultSet;
    private final ResultSetMetaData metaData;

    public RowDefaults(ResultSet resultSet) {
        this.resultSet = resultSet;
        try {
            this.metaData = resultSet.getMetaData();
        } catch (SQLException exception) {
            throw new RuntimeSQLException(exception);
        }
    }

    @Override
    public int columnCount() {
        return RuntimeSQLException.executeWithResult(metaData::getColumnCount);
    }

    @Override
    public <ResultType> ResultType fromBinaryStream(String columnLabel, Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getBinaryStream(columnLabel), streamReader));
    }

    @Override
    public <ResultType> ResultType fromBinaryStream(int columnIndex, Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getBinaryStream(columnIndex), streamReader));
    }

    @Override
    public <ResultType> ResultType fromAsciiStream(String columnLabel, Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getAsciiStream(columnLabel), streamReader));
    }

    @Override
    public <ResultType> ResultType fromAsciiStream(int columnIndex, Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromInputStream(resultSet.getAsciiStream(columnIndex), streamReader));
    }

    @Override
    public <ResultType> ResultType fromCharacterStream(String columnLabel, Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getCharacterStream(columnLabel), streamReader));
    }

    @Override
    public <ResultType> ResultType fromCharacterStream(int columnIndex, Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getCharacterStream(columnIndex), streamReader));
    }

    @Override
    public <ResultType> ResultType fromNCharacterStream(String columnLabel, Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getNCharacterStream(columnLabel), streamReader));
    }

    @Override
    public <ResultType> ResultType fromNCharacterStream(int columnIndex, Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> fromReader(resultSet.getNCharacterStream(columnIndex), streamReader));
    }

    @Override
    public <ResultType> ResultType fromBlob(int columnIndex, Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromBlob(resultSet.getBlob(columnIndex), streamReader));
    }

    @Override
    public <ResultType> ResultType fromBlob(String columnLabel, Function<InputStream, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromBlob(resultSet.getBlob(columnLabel), streamReader));
    }

    @Override
    public <ResultType> ResultType fromClob(int columnIndex, Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromClob(resultSet.getClob(columnIndex), streamReader));
    }

    @Override
    public <ResultType> ResultType fromClob(String columnLabel, Function<Reader, ResultType> streamReader) {
        InvalidArgumentException.throwIfNull(streamReader, "streamReader");
        return RuntimeSQLException.executeWithResult(() -> BlobReaders.fromClob(resultSet.getClob(columnLabel), streamReader));
    }

    @Override
    public byte[] asByteArray(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBytes(columnIndex));
    }

    @Override
    public byte[] asByteArray(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBytes(columnLabel));
    }

    @Override
    public String asString(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getString(columnIndex));
    }

    @Override
    public String asString(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getString(columnLabel));
    }

    @Override
    public Short asShort(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getShort(columnLabel)));
    }

    @Override
    public Short asShort(int column) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getShort(column)));
    }

    @Override
    public Integer asInteger(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getInt(columnLabel)));
    }

    @Override
    public Integer asInteger(int column) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getInt(column)));
    }

    @Override
    public Long asLong(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getLong(columnLabel)));
    }

    @Override
    public Long asLong(int column) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getLong(column)));
    }

    @Override
    public Double asDouble(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getDouble(columnIndex)));
    }

    @Override
    public Double asDouble(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getDouble(columnLabel)));
    }

    @Override
    public Float asFloat(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getFloat(columnLabel)));
    }

    @Override
    public Float asFloat(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getFloat(columnIndex)));
    }

    @Override
    public Byte asByte(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getByte(columnLabel)));
    }

    @Override
    public Byte asByte(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getByte(columnIndex)));
    }

    @Override
    public BigDecimal asBigDecimal(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBigDecimal(columnLabel));
    }

    @Override
    public BigDecimal asBigDecimal(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getBigDecimal(columnIndex));
    }

    @Override
    public Boolean asBoolean(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getBoolean(columnLabel)));
    }

    @Override
    public Boolean asBoolean(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> returnValueOrNull(resultSet.getBoolean(columnIndex)));
    }

    @Override
    public Date asDate(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getDate(columnLabel));
    }

    @Override
    public Date asDate(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getDate(columnIndex));
    }

    @Override
    public Time asTime(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTime(columnLabel));
    }

    @Override
    public Time asTime(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTime(columnIndex));
    }

    @Override
    public Timestamp asTimestamp(String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTimestamp(columnLabel));
    }

    @Override
    public Timestamp asTimestamp(int columnIndex) {
        return RuntimeSQLException.executeWithResult(() -> resultSet.getTimestamp(columnIndex));
    }

    private <ResultType> ResultType returnValueOrNull(ResultType value) throws SQLException {
        if (resultSet.wasNull()) {
            return null;
        }
        return value;
    }
}
