package com.asprotunity.queryiteasy;

import com.asprotunity.queryiteasy.connection.GenericRow;

import javax.sql.DataSource;

public class DefaultDataStore extends DataStore<GenericRow> {
    public DefaultDataStore(DataSource dataSource) {
        super(dataSource, GenericRow::new);
    }
}
