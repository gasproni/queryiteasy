package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.connection.Batch;
import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.disposer.Disposer;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class WrappedJDBCConnection implements Connection, AutoCloseable {
    private final java.sql.Connection connection;

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
    public void update(String sql, InputParameter... parameters) {
        RuntimeSQLException.wrapException(() -> {
            try (Disposer disposer = Disposer.makeNew();
                 PreparedStatement statement = createStatement(connection, sql, disposer, parameters)) {
                statement.execute();
            }
        });
    }

    @Override
    public void update(String sql, List<Batch> batches) {
        if (batches.isEmpty()) {
            throw new RuntimeSQLException("Batch is empty.");
        }
        RuntimeSQLException.wrapException(() -> {
            try (Disposer disposer = Disposer.makeNew();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, disposer);
                }
                statement.executeBatch();
            }
        });
    }


    @Override
    public <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> processRow, InputParameter... parameters) {
        return RuntimeSQLException.wrapExceptionAndReturnResult(() -> {
            try (Disposer disposer = Disposer.makeNew();
                 PreparedStatement statement = createStatement(connection, sql, disposer, parameters)) {
                try (ResultSet rs = statement.executeQuery();
                     Stream<Row> rowStream = StreamSupport.stream(new RowSpliterator(rs), false)) {
                    return processRow.apply(rowStream);
                }
            }
        });
    }

    private static PreparedStatement createStatement(java.sql.Connection connection, String sql, Disposer disposer,
                                                     InputParameter... parameters) throws SQLException {
        PreparedStatement result = connection.prepareStatement(sql);
        bindParameters(parameters, result, disposer);
        return result;
    }

    private static void addBatch(Batch batch, PreparedStatement preparedStatement, Disposer disposer) throws SQLException {
        batch.forEachParameter(bindTo(preparedStatement, disposer));
        preparedStatement.addBatch();
    }

    private static void bindParameters(InputParameter[] parameters, PreparedStatement preparedStatement, Disposer disposer) {
        IntStream.range(0, parameters.length).forEach(i -> bindTo(preparedStatement, disposer).accept(parameters[i], i));
    }

    private static BiConsumer<InputParameter, Integer> bindTo(PreparedStatement preparedStatement, Disposer disposer) {
        return (parameter, position) ->
                parameter.accept(new PositionalParameterBinder(position + 1, preparedStatement, disposer));
    }

}
