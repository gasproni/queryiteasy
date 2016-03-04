package com.asprotunity.queryiteasy.connection;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Supplier;

public interface InputParameterBinder {
    void bind(String value);

    void bind(Short value);

    void bind(Integer value);

    void bind(Long value);

    void bind(Double value);

    void bind(Float value);

    void bind(Byte value);

    void bind(Boolean value);

    void bind(BigDecimal value);

    void bind(Date value);

    void bind(Time value);

    void bind(Timestamp value);

    void bind(ThrowingSupplier<InputStream> value);
}
