package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.closer.Closer;

import java.sql.PreparedStatement;

@FunctionalInterface
public interface InputParameter {

    void bind(PreparedStatement statement, int position, Closer closer);

}
