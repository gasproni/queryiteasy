package com.asprotunity.jdbcunboil.internal;

import com.asprotunity.jdbcunboil.connection.Batch;
import com.asprotunity.jdbcunboil.connection.Connection;
import com.asprotunity.jdbcunboil.connection.Row;
import com.asprotunity.jdbcunboil.connection.StatementParameter;
import com.asprotunity.jdbcunboil.exception.RuntimeSQLException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

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
    public <MappedType> List<MappedType> executeQuery(String sql, Function<Row, MappedType> rowMapper, StatementParameter... parameters) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindParameters(parameters, statement);
                try (ResultSet rs = statement.executeQuery()) {
                    List<MappedType> result = new ArrayList<>();
                    while (rs.next()) {
                        result.add(rowMapper.apply(new WrappedResultSet(rs)));
                    }
                    return result;
                }
            }
        });
    }

    private void addBatch(Batch batch, PreparedStatement preparedStatement) {
        batch.forEachParameter(bindTo(preparedStatement));
        RuntimeSQLException.wrapException(preparedStatement::addBatch);
    }

    private void bindParameters(StatementParameter[] parameters, PreparedStatement preparedStatement) {
        Batch.forEachParameter(parameters, bindTo(preparedStatement));
    }

    private BiConsumer<StatementParameter, Integer> bindTo(PreparedStatement preparedStatement) {
        return (parameter, position) ->
                parameter.accept(new PositionalParameterBinder(position + 1, preparedStatement));
    }

}
