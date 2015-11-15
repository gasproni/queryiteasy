package com.asprotunity.queryiteasy.connection;

public class Batch {

    private final StatementParameter[] parameters;

    public static Batch batch(StatementParameter firstParameter, StatementParameter... parameters) {
        return new Batch(firstParameter, parameters);
    }

    public void forEachParameter(PositionalParameterFunction function) {
        forEachParameter(parameters, function);
    }

    public static void forEachParameter(StatementParameter[] parameters, PositionalParameterFunction function) {
        for (int position = 0; position < parameters.length; ++position) {
            function.apply(parameters[position], position);
        }

    }

    private Batch(StatementParameter parameter, StatementParameter[] parameters) {
        this.parameters = new StatementParameter[parameters.length + 1];
        this.parameters[0] = parameter;
        System.arraycopy(parameters, 0, this.parameters, 1, parameters.length);
    }
}
