package com.asprotunity.queryiteasy.internal.functional;

import com.asprotunity.queryiteasy.functional.ThrowingSupplier;
import com.asprotunity.queryiteasy.functional.ThrowingFunctionException;

public abstract class ThrowingFunctionExceptionWrapper {
    public static <ResultType> ResultType executeAndReturnResult(ThrowingSupplier<ResultType> supplier) {
        try {
            return supplier.get();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new ThrowingFunctionException(exception);
        }
    }
}
