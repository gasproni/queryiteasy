package com.asprotunity.queryiteasy.internal;


import com.asprotunity.queryiteasy.connection.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class WrappedPreparedStatement {

    private PreparedStatement preparedStatement;

    public WrappedPreparedStatement(java.sql.Connection connection, String sql) {
        RuntimeSQLException.wrapException(() -> this.preparedStatement = connection.prepareStatement(sql));

    }

    public void execute(StatementParameter... parameters) {
        bindParameters(parameters);
        RuntimeSQLException.wrapException(preparedStatement::execute);
    }

    public void executeBatch(Batch firstBatch, Batch...batches) {
        addBatch(firstBatch);
        for (Batch batch : batches) {
            addBatch(batch);
        }
        RuntimeSQLException.wrapException(preparedStatement::executeBatch);
    }

    public <ResultType> List<ResultType> executeQuery(RowMapper<ResultType> rowMapper, StatementParameter...parameters) {
        bindParameters(parameters);
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

    private void addBatch(Batch batch) {
        bindParameters(batch.parameters);
        RuntimeSQLException.wrapException(preparedStatement::addBatch);
    }

    private void bindParameters(StatementParameter[] parameters) {
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
        public void setInt(int value) {
            RuntimeSQLException.wrapException(()->statement.setInt(this.position, value));
        }

        @Override
        public void setDouble(double value) {
            RuntimeSQLException.wrapException(()->statement.setDouble(this.position, value));
        }

        private void bind(StatementParameter[] parameters) {
            for (int index = 0; index < parameters.length; ++index) {
                parameters[index].readValue(this);
                position += index + 1;
            }
        }
    }
}
