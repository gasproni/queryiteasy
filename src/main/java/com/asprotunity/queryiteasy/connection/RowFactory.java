package com.asprotunity.queryiteasy.connection;

import java.sql.ResultSet;

@FunctionalInterface
public interface RowFactory<RowType extends Row> {
    RowType make(ResultSet resultSet);
}
