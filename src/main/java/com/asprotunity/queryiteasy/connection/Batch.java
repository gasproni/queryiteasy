package com.asprotunity.queryiteasy.connection;

public class Batch {

    public final PositionalBinder[] binders;

    public static Batch batch(PositionalBinder binder, PositionalBinder... binders) {
        return new Batch(binder, binders);
    }

    private Batch(PositionalBinder binder, PositionalBinder[] binders) {
        this.binders = new PositionalBinder[binders.length + 1];
        this.binders[0] = binder;
        System.arraycopy(binders, 0, this.binders, 1, binders.length);
    }
}
