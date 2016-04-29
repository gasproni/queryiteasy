package com.asprotunity.queryiteasy.connection;


import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.function.Function;

public interface Row {
    String asString(String columnName);
    String asString(int position);
    Short asShort(String columnName);
    Short asShort(int position);
    Integer asInteger(String columnName);
    Integer asInteger(int position);
    Long asLong(String columnName);
    Long asLong(int position);
    Double asDouble(String columnName);
    Double asDouble(int position);
    Float asFloat(String columnName);
    Float asFloat(int position);
    Byte asByte(String columnName);
    Byte asByte(int position);
    BigDecimal asBigDecimal(String columnName);
    BigDecimal asBigDecimal(int position);
    Boolean asBoolean(String columnName);
    Boolean asBoolean(int position);
    Date asDate(String columnName);
    Date asDate(int position);
    Time asTime(String columnName);
    Time asTime(int position);
    Timestamp asTimestamp(String columnName);
    Timestamp asTimestamp(int position);
    <ResultType> ResultType fromBlob(String columnName,
                                     Function<Optional<InputStream>, ResultType> blobInputStream);
    <ResultType> ResultType fromBlob(int position,
                                     Function<Optional<InputStream>, ResultType> blobReader);
}
