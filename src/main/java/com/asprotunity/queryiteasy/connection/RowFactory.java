package com.asprotunity.queryiteasy.connection;

import java.sql.ResultSet;

@FunctionalInterface
public interface RowFactory<RowType> {
    RowType make(ResultSet resultSet);
}
