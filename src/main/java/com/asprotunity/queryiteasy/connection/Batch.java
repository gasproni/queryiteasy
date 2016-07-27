package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;

import java.util.function.BiConsumer;
import java.util.stream.IntStream;

public class Batch {

    private final InputParameter[] parameters;

    private Batch(InputParameter parameter, InputParameter[] parameters) {
        this.parameters = new InputParameter[parameters.length + 1];
        this.parameters[0] = parameter;
        System.arraycopy(parameters, 0, this.parameters, 1, parameters.length);
    }

    public static Batch batch(InputParameter firstParameter, InputParameter... parameters) {
        InvalidArgumentException.throwIfNull(firstParameter, "firstParameter");
        InvalidArgumentException.throwIfNull(parameters, "parameters");

        for (int position = 0; position < parameters.length; ++position) {
            InvalidArgumentException.throwIfNull(parameters[position], "parameters[" + position + 2 + "]");
        }
        return new Batch(firstParameter, parameters);
    }

    public void forEachParameter(BiConsumer<InputParameter, Integer> function) {
        IntStream.range(0, parameters.length).forEach(i -> function.accept(parameters[i], i));
    }
}
