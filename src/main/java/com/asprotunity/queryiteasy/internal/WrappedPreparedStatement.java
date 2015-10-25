package com.asprotunity.queryiteasy.internal;


import com.asprotunity.queryiteasy.connection.RowMapper;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.connection.Statement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class WrappedPreparedStatement implements Statement {

    private PreparedStatement preparedStatement;

    public WrappedPreparedStatement(java.sql.Connection connection, String sql) {
        RuntimeSQLException.wrapException(() -> this.preparedStatement = connection.prepareStatement(sql));

    }

    @Override
    public void execute() {
        RuntimeSQLException.wrapException(preparedStatement::execute);
    }

    @Override
    public <ResultType> List<ResultType> executeQuery(RowMapper<ResultType> rowMapper) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            try (ResultSet rs = preparedStatement.executeQuery()) {
                List<ResultType> result = new ArrayList<>();

                while (rs.next()) {
                    result.add(rowMapper.map(new WrappedResultSet(rs)));
                }

                return result;
            }
        });
    }

    @Override
    public void setString(int position, String value) {
        RuntimeSQLException.wrapException(() ->
                preparedStatement.setString(position, value));
    }

    @Override
    public void setInt(int position, int value) {
        RuntimeSQLException.wrapException(() ->
                preparedStatement.setInt(position, value));
    }

    @Override
    public void setDouble(int position, double value) {
        RuntimeSQLException.wrapException(() ->
                preparedStatement.setDouble(position, value));
    }
}
