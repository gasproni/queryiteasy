package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.closer.Closer;
import com.asprotunity.queryiteasy.connection.*;

import java.sql.CallableStatement;
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
    private final Closer connectionCloser;

    public WrappedJDBCConnection(java.sql.Connection connection) {
        this.connection = connection;
        this.connectionCloser = new Closer();
        RuntimeSQLException.execute(() -> this.connection.setAutoCommit(false));
    }

    public void commit() {
        RuntimeSQLException.execute(connection::commit);
    }

    @Override
    public void close() {
        RuntimeSQLException.execute(() -> {
            connection.rollback();
            connectionCloser.close();
            connection.close();
        });
    }

    @Override
    public void update(String sql, InputParameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 Closer closer = new Closer()) {
                bindParameters(parameters, statement, closer);
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
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 Closer statementCloser = new Closer()) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, statementCloser);
                }
                statement.executeBatch();
            }
        });
    }


    @Override
    public <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> processRow, InputParameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 Closer statementCloser = new Closer()) {
                bindParameters(parameters, statement, statementCloser);
                try (ResultSet rs = statement.executeQuery();
                     Stream<Row> rowStream = StreamSupport.stream(new RowSpliterator(new WrappedJDBCResultSet(rs)), false)) {
                    return processRow.apply(rowStream);
                }
            }
        });
    }

    @Override
    public void call(String sql, Parameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (CallableStatement statement = connection.prepareCall(sql);
                 Closer statementCloser = new Closer()) {
                bindCallableParameters(parameters, statement, statementCloser);
                statement.execute();
            }
        });
    }

    private static void addBatch(Batch batch, PreparedStatement preparedStatement, Closer closer) throws SQLException {
        batch.forEachParameter(bindTo(preparedStatement, closer));
        preparedStatement.addBatch();
    }

    private static void bindParameters(InputParameter[] parameters, PreparedStatement preparedStatement, Closer closer) {
        IntStream.range(0, parameters.length).forEach(i -> parameters[i].bind(preparedStatement, i + 1, closer));
    }

    private static void bindCallableParameters(Parameter[] parameters, CallableStatement callableStatement, Closer closer) {
        IntStream.range(0, parameters.length).forEach(i -> parameters[i].bind(callableStatement, i + 1, closer));
    }

    private static BiConsumer<InputParameter, Integer> bindTo(PreparedStatement preparedStatement, Closer closer) {
        return (parameter, position) ->
                parameter.bind(preparedStatement, position + 1, closer);
    }

}
