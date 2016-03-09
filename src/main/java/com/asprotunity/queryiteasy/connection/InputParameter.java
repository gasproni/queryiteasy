package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.disposer.Closer;

import java.sql.PreparedStatement;

@FunctionalInterface
public interface InputParameter {

    void accept(PreparedStatement statement, int position, Closer closer);

}
