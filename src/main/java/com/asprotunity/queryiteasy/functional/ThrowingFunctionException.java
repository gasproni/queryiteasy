package com.asprotunity.queryiteasy.functional;

public class ThrowingFunctionException extends RuntimeException {
    public ThrowingFunctionException(Exception cause) {
        super(cause);
    }
}
