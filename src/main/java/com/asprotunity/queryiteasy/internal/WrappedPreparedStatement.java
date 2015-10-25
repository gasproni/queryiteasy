package com.asprotunity.queryiteasy.internal;


import com.asprotunity.queryiteasy.connection.*;

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
    public void execute(PositionalBinder... binders) {
        applyBinders(binders);
        RuntimeSQLException.wrapException(preparedStatement::execute);
    }

    @Override
    public void executeBatch(Batch firstBatch, Batch...batches) {
        addBatch(firstBatch);
        for (Batch batch : batches) {
            addBatch(batch);
        }
        RuntimeSQLException.wrapException(preparedStatement::executeBatch);
    }

    @Override
    public <ResultType> List<ResultType> executeQuery(RowMapper<ResultType> rowMapper, PositionalBinder...binders) {
        applyBinders(binders);
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

    private void addBatch(Batch batch) {
        applyBinders(batch.binders);
        RuntimeSQLException.wrapException(preparedStatement::addBatch);
    }

    private void applyBinders(PositionalBinder[] binders) {
        for (int position = 0; position < binders.length; ++position) {
            binders[position].apply(this, position + 1);
        }
    }
}
