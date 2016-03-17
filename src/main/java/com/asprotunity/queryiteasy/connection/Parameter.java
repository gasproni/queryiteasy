package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.closer.Closer;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;

public interface Parameter {

    void bind(CallableStatement statement, int position, Closer closer);

}
