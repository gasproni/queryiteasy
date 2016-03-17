package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.closer.Closer;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringInputOutputParameter extends InputOutputParameter<String> {

    public StringInputOutputParameter(String value) {
        super(value);
    }

    @Override
    public void bind(CallableStatement statement, int position, Closer closer) {
        RuntimeSQLException.execute(() -> {
            statement.setString(position, value());
            statement.registerOutParameter(position, Types.VARCHAR);
            closer.onClose(() -> setValue(statement.getString(position)));
        });
    }
}
