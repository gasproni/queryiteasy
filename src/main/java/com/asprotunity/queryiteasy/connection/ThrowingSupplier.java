package com.asprotunity.queryiteasy.connection;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}
