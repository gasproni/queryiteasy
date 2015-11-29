package com.asprotunity.jdbcunboil.connection;

import com.asprotunity.jdbcunboil.exception.InvalidArgumentException;

import java.util.function.BiConsumer;

public class Batch {

    private final StatementParameter[] parameters;

    public static Batch batch(StatementParameter firstParameter, StatementParameter... parameters) {
        if (firstParameter == null || parameters == null) {
            throw new InvalidArgumentException("Arguments cannot be null.");
        }
        for (int position = 0; position < parameters.length; ++position) {
            if (parameters[position] == null) {
                throw new InvalidArgumentException("Arguments cannot be null: null parameter at position " + position + 2);
            }
        }
        return new Batch(firstParameter, parameters);
    }

    public void forEachParameter(BiConsumer<StatementParameter, Integer> function) {
        forEachParameter(parameters, function);
    }

    public static void forEachParameter(StatementParameter[] parameters, BiConsumer<StatementParameter, Integer> function) {
        for (int position = 0; position < parameters.length; ++position) {
            function.accept(parameters[position], position);
        }

    }

    private Batch(StatementParameter parameter, StatementParameter[] parameters) {
        this.parameters = new StatementParameter[parameters.length + 1];
        this.parameters[0] = parameter;
        System.arraycopy(parameters, 0, this.parameters, 1, parameters.length);
    }
}
