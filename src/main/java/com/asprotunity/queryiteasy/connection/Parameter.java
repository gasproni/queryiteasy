package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;

public interface Parameter {

    void bind(CallableStatement statement, int position, Scope queryScope);

}
