package com.asprotunity.queryiteasy.connection;

public class ThrowingSupplierException extends RuntimeException {
    public ThrowingSupplierException(Exception cause) {
        super(cause);
    }

    public static <ResultType> ResultType wrapExceptionAndReturnResult(CodeBlock<ResultType> codeBlock) {
        try {
            return codeBlock.executeAndReturnResult();
        } catch (Exception e) {
            throw new ThrowingSupplierException(e);
        }
    }

    @FunctionalInterface
    public interface CodeBlock<ResultType> {
        ResultType executeAndReturnResult() throws Exception;
    }
}
