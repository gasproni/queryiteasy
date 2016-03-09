package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.*;
import com.asprotunity.queryiteasy.disposer.Closer;

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
        RuntimeSQLException.execute(() -> this.connection.setAutoCommit(false));
    }

    public void commit() {
        RuntimeSQLException.execute(connection::commit);
    }

    @Override
    public void close() {
        RuntimeSQLException.execute(() -> {
            connection.rollback();
            connection.close();
        });
    }

    @Override
    public void update(String sql, InputParameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (Closer closer = new Closer();
                 PreparedStatement statement = createStatement(connection, sql, closer, parameters)) {
                statement.execute();
            }
        });
    }

    @Override
    public void update(String sql, List<Batch> batches) {
        if (batches.isEmpty()) {
            throw new RuntimeSQLException("Batch is empty.");
        }
        RuntimeSQLException.execute(() -> {
            try (Closer closer = new Closer();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, closer);
                }
                statement.executeBatch();
            }
        });
    }


    @Override
    public <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> processRow, InputParameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            try (Closer closer = new Closer();
                 PreparedStatement statement = createStatement(connection, sql, closer, parameters)) {
                try (ResultSet rs = statement.executeQuery();
                     Stream<Row> rowStream = StreamSupport.stream(new RowSpliterator(rs), false)) {
                    return processRow.apply(rowStream);
                }
            }
        });
    }

    private static PreparedStatement createStatement(java.sql.Connection connection, String sql, Closer closer,
                                                     InputParameter... parameters) throws SQLException {
        PreparedStatement result = connection.prepareStatement(sql);
        bindParameters(parameters, result, closer);
        return result;
    }

    private static void addBatch(Batch batch, PreparedStatement preparedStatement, Closer closer) throws SQLException {
        batch.forEachParameter(bindTo(preparedStatement, closer));
        preparedStatement.addBatch();
    }

    private static void bindParameters(InputParameter[] parameters, PreparedStatement preparedStatement, Closer closer) {
        IntStream.range(0, parameters.length).forEach(i -> bindTo(preparedStatement, closer).accept(parameters[i], i));
    }

    private static BiConsumer<InputParameter, Integer> bindTo(PreparedStatement preparedStatement, Closer closer) {
        return (parameter, position) ->
                parameter.accept(preparedStatement, position +1, closer);
    }

}
