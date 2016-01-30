package com.asprotunity.jdbcunboiled.internal;

import com.asprotunity.jdbcunboiled.connection.Batch;
import com.asprotunity.jdbcunboiled.connection.Connection;
import com.asprotunity.jdbcunboiled.connection.Row;
import com.asprotunity.jdbcunboiled.connection.StatementParameter;
import com.asprotunity.jdbcunboiled.exception.RuntimeSQLException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public void update(String sql, StatementParameter... parameters) {
        RuntimeSQLException.wrapException(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindParameters(parameters, statement);
                statement.execute();
            }
        });
    }

    @Override
    public void update(String sql, Batch firstBatch, Batch... batches) {
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
    public <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> rowProcessor, StatementParameter... parameters) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindParameters(parameters, statement);
                try (ResultSet rs = statement.executeQuery()) {
                    return rowProcessor.apply(StreamSupport.stream(new RowSpliterator(rs), false));
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
