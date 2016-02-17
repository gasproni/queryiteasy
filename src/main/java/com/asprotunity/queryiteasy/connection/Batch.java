package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;

import java.util.function.BiConsumer;
import java.util.stream.IntStream;

public class Batch {

    private final InputParameter[] parameters;

    public static Batch batch(InputParameter firstParameter, InputParameter... parameters) {
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

    public void forEachParameter(BiConsumer<InputParameter, Integer> function) {
        IntStream.range(0, parameters.length).forEach(i -> function.accept(parameters[i], i));
    }

    private Batch(InputParameter parameter, InputParameter[] parameters) {
        this.parameters = new InputParameter[parameters.length + 1];
        this.parameters[0] = parameter;
        System.arraycopy(parameters, 0, this.parameters, 1, parameters.length);
    }
}
