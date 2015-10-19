package com.asprotunity.queryiteasy;

public class SQLException extends RuntimeException {
    public SQLException(java.sql.SQLException e) {
        super(e);
    }

    public static void wrapException(VoidCodeBlock codeBlock) {
        try {
            codeBlock.get();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public static <ResultType> ResultType wrapExceptionAndReturnResult(CodeBlock<ResultType> codeBlock) {
        try {
            return codeBlock.get();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @FunctionalInterface
    public interface CodeBlock<ResultType> {
        public ResultType get() throws java.sql.SQLException;
    }

    @FunctionalInterface
    public interface VoidCodeBlock {
        public void get() throws java.sql.SQLException;
    }

}
