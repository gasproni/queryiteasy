package com.asprotunity.queryiteasy.connection;


import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

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
}
