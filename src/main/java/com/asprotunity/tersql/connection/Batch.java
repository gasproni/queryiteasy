package com.asprotunity.tersql.connection;

import com.asprotunity.tersql.exception.InvalidArgumentException;

import java.util.function.BiConsumer;
import java.util.stream.IntStream;

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
        IntStream.range(0, parameters.length).forEach(i -> function.accept(parameters[i], i));
    }

    private Batch(StatementParameter parameter, StatementParameter[] parameters) {
        this.parameters = new StatementParameter[parameters.length + 1];
        this.parameters[0] = parameter;
        System.arraycopy(parameters, 0, this.parameters, 1, parameters.length);
    }
}
