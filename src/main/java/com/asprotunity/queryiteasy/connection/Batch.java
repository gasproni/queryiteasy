package com.asprotunity.queryiteasy.connection;

public class Batch {

    public final StatementParameter[] parameters;

    public static Batch batch(StatementParameter binder, StatementParameter... binders) {
        return new Batch(binder, binders);
    }

    private Batch(StatementParameter parameter, StatementParameter[] parameters) {
        this.parameters = new StatementParameter[parameters.length + 1];
        this.parameters[0] = parameter;
        System.arraycopy(parameters, 0, this.parameters, 1, parameters.length);
    }
}
