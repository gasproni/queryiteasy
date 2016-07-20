package com.asprotunity.queryiteasy.connection;


import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * Tag interface implemented by all output parameter classes
 */
public interface OutputParameter extends Parameter {

    @FunctionalInterface
    interface ThrowingBiFunction<T1, T2, ResultType, ExceptionType extends Exception> {
        ResultType apply(T1 p1, T2 p2) throws ExceptionType;
    }

    static  <ResultType> ResultType returnValueOrNull(CallableStatement statement, int position,
                                                      ThrowingBiFunction<CallableStatement, Integer, ResultType, SQLException> readValue) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            ResultType result = readValue.apply(statement, position);
            if (statement.wasNull()) {
                return null;
            }
            return result;
        });
    }
}
