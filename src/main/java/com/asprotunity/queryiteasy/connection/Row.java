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
    Short asShort(String columnName);
    Integer asInteger(String columnName);
    Long asLong(String columnName);
    Double asDouble(String columnName);
    Float asFloat(String columnName);
    Byte asByte(String columnName);
    BigDecimal asBigDecimal(String columnName);
    Boolean asBoolean(String columnName);
    Date asDate(String columnName);
    Time asTime(String columnName);
    Timestamp asTimestamp(String columnName);
    <ResultType> ResultType fromBlob(String columnName, Function<Optional<InputStream>, ResultType> blobInputStream);
}
