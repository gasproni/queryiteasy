package com.asprotunity.queryiteasy.internal.connection;

import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetWrapperFactory {
    ResultSetWrapper make(ResultSet resultSet);
}
