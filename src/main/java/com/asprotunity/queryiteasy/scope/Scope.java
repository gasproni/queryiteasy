package com.asprotunity.queryiteasy.scope;

public interface Scope {
    @FunctionalInterface
    interface ThrowingConsumer<T> {
        void apply(T obj) throws Exception;
    }
    void onLeave(LeaveAction caller);

    <T> T make(T obj, ThrowingConsumer<T> consumer);
}
