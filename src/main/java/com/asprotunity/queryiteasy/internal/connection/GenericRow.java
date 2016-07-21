package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.RowDefaults;

import java.sql.ResultSet;

public final class GenericRow extends RowDefaults {

    public GenericRow(ResultSet resultSet) {
        super(resultSet);
    }

}
