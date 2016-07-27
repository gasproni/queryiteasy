package com.asprotunity.queryiteasy.exception;

public class RuntimeSQLException extends RuntimeException {

    public RuntimeSQLException(java.sql.SQLException cause) {
        super(cause);
    }

    public RuntimeSQLException(String message) {
        super(message);
    }

    public static void execute(ThrowingCodeBlock codeBlock) {
        try {
            codeBlock.execute();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    public static <ResultType> ResultType executeWithResult(ThrowingSupplier<ResultType> throwingSupplier) {
        try {
            return throwingSupplier.executeWithResult();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    @FunctionalInterface
    public interface ThrowingSupplier<ResultType> {
        ResultType executeWithResult() throws java.sql.SQLException;
    }

    @FunctionalInterface
    public interface ThrowingCodeBlock {
        void execute() throws java.sql.SQLException;
    }
}
