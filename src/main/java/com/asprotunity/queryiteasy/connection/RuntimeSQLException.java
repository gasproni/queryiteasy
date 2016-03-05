package com.asprotunity.queryiteasy.connection;

public class RuntimeSQLException extends RuntimeException {

    public RuntimeSQLException(java.sql.SQLException cause) {
        super(cause);
    }

    public RuntimeSQLException(String message) {
        super(message);
    }

    public static void execute(VoidCodeBlock codeBlock) {
        try {
            codeBlock.execute();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    public static <ResultType> ResultType executeAndReturnResult(ThrowingSupplier<ResultType> throwingSupplier) {
        try {
            return throwingSupplier.executeAndReturnResult();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    @FunctionalInterface
    public interface ThrowingSupplier<ResultType> {
        ResultType executeAndReturnResult() throws java.sql.SQLException;
    }

    @FunctionalInterface
    public interface VoidCodeBlock {
        void execute() throws java.sql.SQLException;
    }
}
