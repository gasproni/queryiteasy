package com.asprotunity.queryiteasy.connection.internal;

import com.asprotunity.queryiteasy.connection.Batch;
import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.Parameter;
import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.AutoCloseableScope;
import com.asprotunity.queryiteasy.scope.DefaultAutoCloseableScope;
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
    private final AutoCloseableScope connectionScope;

    public WrappedJDBCConnection(java.sql.Connection connection) {
        this(connection, new DefaultAutoCloseableScope());
    }

    /**
     * This is for testing purposes only
     */
    WrappedJDBCConnection(java.sql.Connection connection, AutoCloseableScope connectionScope) {
        this.connection = connection;
        this.connectionScope = connectionScope;
        RuntimeSQLException.execute(() -> this.connection.setAutoCommit(false));
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
        InvalidArgumentException.throwIfNull(sql, "sql");
        InvalidArgumentException.throwIf(sql.isEmpty(), "sql cannot be empty.");
        InvalidArgumentException.throwIfNull(parameters, "parameters");
        RuntimeSQLException.execute(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 DefaultAutoCloseableScope scope = new DefaultAutoCloseableScope()) {
                bindParameters(parameters, statement, scope);
                statement.execute();
            }
        });
    }

    @Override
    public void update(String sql, List<Batch> batches) {
        InvalidArgumentException.throwIfNull(sql, "sql");
        InvalidArgumentException.throwIf(sql.isEmpty(), "sql cannot be empty.");
        InvalidArgumentException.throwIfNull(batches, "batches");
        InvalidArgumentException.throwIf(batches.isEmpty(), "batches cannot be empty.");
        RuntimeSQLException.execute(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 DefaultAutoCloseableScope queryScope = new DefaultAutoCloseableScope()) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, queryScope);
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public <MappedRowType> Stream<MappedRowType> select(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                                        InputParameter... parameters) {
        InvalidArgumentException.throwIfNull(rowMapper, "rowMapper");
        InvalidArgumentException.throwIfNull(sql, "sql");
        InvalidArgumentException.throwIf(sql.isEmpty(), "sql cannot be empty.");
        InvalidArgumentException.throwIfNull(parameters, "parameters");
        return RuntimeSQLException.executeWithResult(() -> {
            DefaultAutoCloseableScope resultSetAndStatementScope = connectionScope.add(new DefaultAutoCloseableScope(),
                                                                                       DefaultAutoCloseableScope::close);
            try (DefaultAutoCloseableScope executeQueryScope = new DefaultAutoCloseableScope()) {
                PreparedStatement statement = resultSetAndStatementScope.add(connection.prepareStatement(sql),
                                                                             PreparedStatement::close);
                bindParameters(parameters, statement, executeQueryScope);
                return executeQuery(rowMapper, resultSetAndStatementScope, statement);
            } catch (Exception ex) {
                resultSetAndStatementScope.close();
                throw ex;
            }
        });
    }

    @Override
    public void call(String sql, Parameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (CallableStatement statement = connection.prepareCall(sql);
                 DefaultAutoCloseableScope queryScope = new DefaultAutoCloseableScope()) {
                bindCallableParameters(parameters, statement, queryScope);
                statement.execute();
            }
        });
    }

    @Override
    public <MappedRowType> Stream<MappedRowType> call(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                                      Parameter... parameters) {
        return RuntimeSQLException.executeWithResult(() -> {
            DefaultAutoCloseableScope resultSetAndStatementScope =
                    connectionScope.add(new DefaultAutoCloseableScope(), DefaultAutoCloseableScope::close);
            try (DefaultAutoCloseableScope executeQueryScope = new DefaultAutoCloseableScope()) {
                CallableStatement statement = resultSetAndStatementScope.add(connection.prepareCall(sql), CallableStatement::close);
                bindCallableParameters(parameters, statement, executeQueryScope);
                return executeQuery(rowMapper, resultSetAndStatementScope, statement);
            } catch (Exception ex) {
                resultSetAndStatementScope.close();
                throw ex;
            }
        });
    }

    private <MapperRowType> Stream<MapperRowType> executeQuery(Function<ResultSet, MapperRowType> rowMapper,
                                                               DefaultAutoCloseableScope resultSetAndStatementScope,
                                                               PreparedStatement statement) throws SQLException {
        ResultSet resultSet = resultSetAndStatementScope.add(statement.executeQuery(), ResultSet::close);
        return StreamSupport.stream(new ResultSetSpliterator(resultSet),
                false)
                .onClose(resultSetAndStatementScope::close)
                .map(rowMapper);
    }

}
