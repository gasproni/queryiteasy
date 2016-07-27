package com.asprotunity.queryiteasy.exception;

public class InvalidArgumentException extends RuntimeException {
    public InvalidArgumentException(String message) {
        super(message);
    }

    public static void throwIf(boolean expression, String message) {
        if (expression) {
            throw new InvalidArgumentException(message);
        }
    }

    public static void throwIfNull(Object object, String parameterName) {
        throwIf(object == null, parameterName + " cannot be null.");
    }
}
