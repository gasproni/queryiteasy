package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;

/**
 * This is the root interface for all parameter types that can be passed to the methods
 * of the {@link Connection} interface.
 */
public interface Parameter {
    void bind(CallableStatement statement, int position, Scope queryScope);

}
