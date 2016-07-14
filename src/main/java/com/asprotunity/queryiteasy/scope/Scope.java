package com.asprotunity.queryiteasy.scope;

public interface Scope {
    @FunctionalInterface
    interface ThrowingConsumer<T> {
        void apply(T obj) throws Exception;
    }

    void add(OnCloseAction caller);

    <T> T add(T obj, ThrowingConsumer<T> consumer);
}
