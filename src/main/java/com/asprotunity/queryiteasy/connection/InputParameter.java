package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;

@FunctionalInterface
public interface InputParameter extends Parameter {

    void bind(PreparedStatement statement, int position, Scope statementScope);

    @Override
    default void bind(CallableStatement statement, int position, Scope statementScope) {
        bind((PreparedStatement) statement, position, statementScope);
    }

}
