package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.*;
import com.asprotunity.queryiteasy.scope.Scope;

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
    private final Scope connectionScope;
    private final ResultSetWrapperFactory resultSetWrapperFactory;

    public WrappedJDBCConnection(java.sql.Connection connection, ResultSetWrapperFactory resultSetWrapperFactory) {
        this.connection = connection;
        this.connectionScope = new Scope();
        this.resultSetWrapperFactory = resultSetWrapperFactory;
        RuntimeSQLException.execute(() -> this.connection.setAutoCommit(false));
    }

    public void commit() {
        RuntimeSQLException.execute(connection::commit);
    }

    @Override
    public void close() {
        RuntimeSQLException.execute(() -> {
            connection.rollback();
            connectionScope.close();
            connection.close();
        });
    }

    @Override
    public void update(String sql, InputParameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 Scope scope = new Scope()) {
                bindParameters(parameters, statement, scope);
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
                 Scope statementScope = new Scope()) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, statementScope);
                }
                statement.executeBatch();
            }
        });
    }


    @Override
    public <ResultType> ResultType select(String sql, Function<Stream<Row>, ResultType> rowProcessor, InputParameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 Scope statementScope = new Scope()) {
                bindParameters(parameters, statement, statementScope);
                return executeQuery(rowProcessor, statement);
            }
        });
    }

    @Override
    public void call(String sql, Parameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (CallableStatement statement = connection.prepareCall(sql);
                 Scope statementScope = new Scope()) {
                bindCallableParameters(parameters, statement, statementScope);
                statement.execute();
            }
        });
    }

    @Override
    public <ResultType> ResultType call(String sql, Function<Stream<Row>, ResultType> rowProcessor, Parameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            try (CallableStatement statement = connection.prepareCall(sql);
                 Scope statementScope = new Scope()) {
                bindCallableParameters(parameters, statement, statementScope);
                return executeQuery(rowProcessor, statement);
            }
        });
    }

    private <ResultType> ResultType executeQuery(Function<Stream<Row>, ResultType> processRow, PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery();
             Stream<Row> rowStream =
                     StreamSupport.stream(new RowSpliterator(resultSetWrapperFactory.make(rs), connectionScope),
                             false)) {
            return processRow.apply(rowStream);
        }
    }

    private static void addBatch(Batch batch, PreparedStatement preparedStatement, Scope scope) throws SQLException {
        batch.forEachParameter(bindTo(preparedStatement, scope));
        preparedStatement.addBatch();
    }

    private static void bindParameters(InputParameter[] parameters, PreparedStatement preparedStatement, Scope scope) {
        IntStream.range(0, parameters.length).forEach(i -> parameters[i].bind(preparedStatement, i + 1, scope));
    }

    private static void bindCallableParameters(Parameter[] parameters, CallableStatement callableStatement, Scope scope) {
        IntStream.range(0, parameters.length).forEach(i -> parameters[i].bind(callableStatement, i + 1, scope));
    }

    private static BiConsumer<InputParameter, Integer> bindTo(PreparedStatement preparedStatement, Scope scope) {
        return (parameter, position) ->
                parameter.bind(preparedStatement, position + 1, scope);
    }

}
