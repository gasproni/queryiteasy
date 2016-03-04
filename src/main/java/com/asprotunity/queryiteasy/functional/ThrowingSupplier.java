package com.asprotunity.queryiteasy.functional;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Exception;
}
