package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.closer.Closer;

import java.sql.CallableStatement;

public interface Parameter {

    void bind(CallableStatement statement, int position, Closer closer);

}
