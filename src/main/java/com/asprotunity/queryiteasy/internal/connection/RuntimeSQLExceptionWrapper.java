package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

public abstract class RuntimeSQLExceptionWrapper {
    public static void execute(VoidCodeBlock codeBlock) {
        try {
            codeBlock.execute();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    public static <ResultType> ResultType executeAndReturnResult(CodeBlock<ResultType> codeBlock) {
        try {
            return codeBlock.executeAndReturnResult();
        } catch (java.sql.SQLException e) {
            throw new RuntimeSQLException(e);
        }
    }

    @FunctionalInterface
    public interface CodeBlock<ResultType> {
        ResultType executeAndReturnResult() throws java.sql.SQLException;
    }

    @FunctionalInterface
    public interface VoidCodeBlock {
        void execute() throws java.sql.SQLException;
    }
}
