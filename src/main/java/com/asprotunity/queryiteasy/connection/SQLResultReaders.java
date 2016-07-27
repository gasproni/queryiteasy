package com.asprotunity.queryiteasy.connection;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLResultReaders {

    public static <ResultType> ResultType
    returnValueOrNull(CallableStatement statement, int position,
                      ThrowingBiFunction<CallableStatement, Integer, ResultType> readValue)
            throws SQLException {
        ResultType result = readValue.apply(statement, position);
        if (statement.wasNull()) {
            return null;
        }
        return result;
    }

    public static <ResultType, PositionType> ResultType
    returnValueOrNull(ResultSet resultSet, PositionType position,
                      ThrowingBiFunction<ResultSet, PositionType, ResultType> readValue)
            throws SQLException {
        ResultType result = readValue.apply(resultSet, position);
        if (resultSet.wasNull()) {
            return null;
        }
        return result;
    }

    @FunctionalInterface
    public interface ThrowingBiFunction<T1, T2, ResultType> {
        ResultType apply(T1 p1, T2 p2) throws SQLException;
    }
}
