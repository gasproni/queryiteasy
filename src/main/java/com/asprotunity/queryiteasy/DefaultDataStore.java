package com.asprotunity.queryiteasy;

import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.internal.connection.RowFromResultSet;

import javax.sql.DataSource;

public class DefaultDataStore extends DataStore<Row> {
    public DefaultDataStore(DataSource dataSource) {
        super(dataSource, RowFromResultSet::new);
    }
}
