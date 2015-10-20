package com.asprotunity.queryiteasy;

public class RuntimeSQLException extends RuntimeException {
    public RuntimeSQLException(java.sql.SQLException e) {
        super(e);
    }

    public static void wrapException(VoidCodeBlock codeBlock) {
        try {
            codeBlock.execute();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    public static <ResultType> ResultType wrapExceptionAndReturnResult(CodeBlock<ResultType> codeBlock) {
        try {
            return codeBlock.executeAndReturnResult();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    @FunctionalInterface
    public interface CodeBlock<ResultType> {
        public ResultType executeAndReturnResult() throws java.sql.SQLException;
    }

    @FunctionalInterface
    public interface VoidCodeBlock {
        public void execute() throws java.sql.SQLException;
    }

}
