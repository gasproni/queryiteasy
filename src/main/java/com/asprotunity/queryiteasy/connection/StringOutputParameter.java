package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.closer.Closer;

import java.sql.CallableStatement;
import java.sql.Types;

public class StringOutputParameter extends OutputParameter<String> {

    @Override
    public void bind(CallableStatement statement, int position, Closer closer) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.VARCHAR);
            closer.onClose(() -> setValue(statement.getString(position)));
        });
    }
}
