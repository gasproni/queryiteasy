package com.asprotunity.queryiteasy.connection;


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Function;

public interface Row {

    int columnCount();

    <ResultType> ResultType fromBinaryStream(int columnIndex, Function<InputStream, ResultType> streamReader);

    <ResultType> ResultType fromBinaryStream(String columnLabel, Function<InputStream, ResultType> streamReader);

    <ResultType> ResultType fromAsciiStream(int columnIndex, Function<InputStream, ResultType> streamReader);

    <ResultType> ResultType fromAsciiStream(String columnLabel, Function<InputStream, ResultType> streamReader);

    <ResultType> ResultType fromCharacterStream(int columnIndex, Function<Reader, ResultType> streamReader);

    <ResultType> ResultType fromCharacterStream(String columnLabel, Function<Reader, ResultType> streamReader);

    <ResultType> ResultType fromNCharacterStream(int columnIndex, Function<Reader, ResultType> streamReader);

    <ResultType> ResultType fromNCharacterStream(String columnLabel, Function<Reader, ResultType> streamReader);

    <ResultType> ResultType fromBlob(int columnIndex, Function<InputStream, ResultType> blobReader);

    <ResultType> ResultType fromBlob(String columnLabel, Function<InputStream, ResultType> blobReader);

    <ResultType> ResultType fromClob(int columnIndex, Function<Reader, ResultType> clobReader);

    <ResultType> ResultType fromClob(String columnLabel, Function<Reader, ResultType> clobReader);

    byte[] asByteArray(int columnIndex);

    byte[] asByteArray(String columnLabel);

    String asString(String columnLabel);

    String asString(int column);

    Short asShort(String columnLabel);

    Short asShort(int column);

    Integer asInteger(String columnLabel);

    Integer asInteger(int column);

    Long asLong(String columnLabel);

    Long asLong(int column);

    Double asDouble(int columnIndex);

    Double asDouble(String columnLabel);

    Float asFloat(String columnLabel);

    Float asFloat(int columnIndex);

    Byte asByte(String columnLabel);

    Byte asByte(int columnIndex);

    BigDecimal asBigDecimal(String columnLabel);

    BigDecimal asBigDecimal(int columnIndex);

    Boolean asBoolean(String columnLabel);

    Boolean asBoolean(int columnIndex);

    Date asDate(String columnLabel);

    Date asDate(int columnIndex);

    Time asTime(String columnLabel);

    Time asTime(int columnIndex);

    Timestamp asTimestamp(String columnLabel);

    Timestamp asTimestamp(int columnIndex);

}
