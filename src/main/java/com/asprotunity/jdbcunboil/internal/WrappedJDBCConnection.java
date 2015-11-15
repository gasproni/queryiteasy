package com.asprotunity.jdbcunboil.internal;

import com.asprotunity.jdbcunboil.connection.*;
import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;
import com.asprotunity.jdbcunboil.userprovided.RowMapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class WrappedJDBCConnection implements Connection, AutoCloseable {
    private java.sql.Connection connection;

    public WrappedJDBCConnection(java.sql.Connection connection) {
        this.connection = connection;
        RuntimeSQLException.wrapException(() -> this.connection.setAutoCommit(false));
    }

    public void commit() {
        RuntimeSQLException.wrapException(connection::commit);
    }

    @Override
    public void close() {
        RuntimeSQLException.wrapException(() -> {
            connection.rollback();
            connection.close();
        });
    }

    @Override
    public void executeUpdate(String sql, StatementParameter... parameters) {
        RuntimeSQLException.wrapException(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindParameters(parameters, statement);
                statement.execute();
            }
        });
    }

    @Override
    public void executeUpdate(String sql, Batch firstBatch, Batch... batches) {
        RuntimeSQLException.wrapException(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                addBatch(firstBatch, statement);
                for (Batch batch : batches) {
                    addBatch(batch, statement);
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper, StatementParameter... parameters) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindParameters(parameters, statement);
                try (ResultSet rs = statement.executeQuery()) {
                    List<ResultType> result = new ArrayList<>();
                    while (rs.next()) {
                        result.add(rowMapper.map(new WrappedResultSet(rs)));
                    }
                    return result;
                }
            }
        });
    }

    private void addBatch(Batch batch, PreparedStatement preparedStatement) {
        batch.forEachParameter(positionalParameterAction(preparedStatement));
        RuntimeSQLException.wrapException(preparedStatement::addBatch);
    }

    private void bindParameters(StatementParameter[] parameters, PreparedStatement preparedStatement) {
        Batch.forEachParameter(parameters, positionalParameterAction(preparedStatement));
    }

    private PositionalParameterFunction positionalParameterAction(PreparedStatement preparedStatement) {
        return (parameter, position) ->
                parameter.apply(new PositionalParameterAction(position + 1, preparedStatement));
    }

    static class PositionalParameterAction implements StatementParameterFunction {

        private PreparedStatement statement;
        private int position;

        private PositionalParameterAction(int position, PreparedStatement statement) {
            this.statement = statement;
            this.position = position;
        }

        @Override
        public void applyTo(String value) {
            RuntimeSQLException.wrapException(() -> statement.setString(this.position, value));
        }

        @Override
        public void applyTo(Integer value) {
            RuntimeSQLException.wrapException(() -> {
                if (value != null) {
                    statement.setInt(this.position, value);
                } else {
                    statement.setNull(this.position, Types.INTEGER);
                }

            });
        }

        @Override
        public void applyTo(Double value) {
            RuntimeSQLException.wrapException(() -> statement.setDouble(this.position, value));
        }

    }

}
