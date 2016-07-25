package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.SQLException;

public interface Parameter {

    void bind(CallableStatement statement, int position, Scope queryScope);

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

    @FunctionalInterface
    interface ThrowingBiFunction<T1, T2, ResultType, ExceptionType extends Exception> {
        ResultType apply(T1 p1, T2 p2) throws ExceptionType;
    }
}
