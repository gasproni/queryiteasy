package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.*;

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
                    PreparedStatement statement = connection.prepareStatement(sql);
                    bindParameters(parameters, statement);
                    statement.execute();
                }
        );
    }

    @Override
    public void executeBatchUpdate(String sql, Batch firstBatch, Batch... batches) {
        RuntimeSQLException.wrapException(() -> {
                    PreparedStatement statement = connection.prepareStatement(sql);
                    addBatch(firstBatch, statement);
                    for (Batch batch : batches) {
                        addBatch(batch, statement);
                    }
                    statement.executeBatch();
                }
        );
    }

    @Override
    public <ResultType> List<ResultType> executeQuery(String sql, RowMapper<ResultType> rowMapper, StatementParameter... parameters) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            PreparedStatement statement = connection.prepareStatement(sql);
            bindParameters(parameters, statement);
            try (ResultSet rs = statement.executeQuery()) {
                List<ResultType> result = new ArrayList<>();

                while (rs.next()) {
                    result.add(rowMapper.map(new WrappedResultSet(rs)));
                }

                return result;
            }
        });
    }

    private void addBatch(Batch batch, PreparedStatement preparedStatement) {
        bindParameters(batch.parameters, preparedStatement);
        RuntimeSQLException.wrapException(preparedStatement::addBatch);
    }

    private void bindParameters(StatementParameter[] parameters, PreparedStatement preparedStatement) {
        PositionalParameterBinder.bind(parameters, preparedStatement);
    }

    static class PositionalParameterBinder implements StatementParameterReader {

        private PreparedStatement statement;
        private int position;

        private PositionalParameterBinder(PreparedStatement statement) {
            this.statement = statement;
            this.position = 1;
        }

        public static void bind(StatementParameter[] parameters, PreparedStatement preparedStatement) {
            PositionalParameterBinder binder = new PositionalParameterBinder(preparedStatement);
            binder.bind(parameters);
        }

        @Override
        public void setString(String value) {
            RuntimeSQLException.wrapException(() -> statement.setString(this.position, value));
        }

        @Override
        public void setInteger(Integer value) {
            RuntimeSQLException.wrapException(() -> {
                if (value != null) {
                    statement.setInt(this.position, value);
                }
                else {
                    statement.setNull(this.position, Types.INTEGER);
                }

            });
        }

        @Override
        public void setDouble(Double value) {
            RuntimeSQLException.wrapException(() -> statement.setDouble(this.position, value));
        }

        private void bind(StatementParameter[] parameters) {
            for (int index = 0; index < parameters.length; ++index) {
                parameters[index].readValue(this);
                position += index + 1;
            }
        }
    }
}
