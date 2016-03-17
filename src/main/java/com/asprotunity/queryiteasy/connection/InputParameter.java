package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.closer.Closer;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;

@FunctionalInterface
public interface InputParameter extends Parameter {

    void bind(PreparedStatement statement, int position, Closer closer);

    @Override
    default void bind(CallableStatement statement, int position, Closer closer) {
        bind((PreparedStatement) statement, position, closer);
    }

}
