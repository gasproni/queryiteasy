package com.asprotunity.queryiteasy.scope;

public interface Scope {
    void add(OnCloseAction caller);

    <T> T add(T obj, OnCloseActionConsumer<T> consumer);

    @FunctionalInterface
    interface OnCloseActionConsumer<T> {
        void apply(T obj) throws Exception;
    }

    @FunctionalInterface
    interface OnCloseAction {
        void perform() throws Exception;
    }
}
