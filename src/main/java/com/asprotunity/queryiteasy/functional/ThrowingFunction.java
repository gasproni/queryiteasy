package com.asprotunity.queryiteasy.functional;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(T value) throws Exception;
}
