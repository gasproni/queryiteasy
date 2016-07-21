package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.*;
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

public class WrappedJDBCConnection<RowType> implements Connection<RowType>, AutoCloseable {
    private final java.sql.Connection connection;
    private final AutoCloseableScope connectionScope;
    private RowFactory<RowType> rowFactory;

    public WrappedJDBCConnection(java.sql.Connection connection, RowFactory<RowType> rowFactory) {
        this(connection, new DefaultAutoCloseableScope(), rowFactory);
    }

    /**
     * This is for testing purposes only
     */
    WrappedJDBCConnection(java.sql.Connection connection, AutoCloseableScope connectionScope, RowFactory<RowType> rowFactory) {
        this.connection = connection;
        this.rowFactory = rowFactory;
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
        if (batches.isEmpty()) {
            throw new RuntimeSQLException("Batch is empty.");
        }
        RuntimeSQLException.execute(() -> {
            try (PreparedStatement statement = connection.prepareStatement(sql);
                 DefaultAutoCloseableScope statementScope = new DefaultAutoCloseableScope()) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, statementScope);
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public <MappedRowType> Stream<MappedRowType> select(Function<RowType, MappedRowType> rowMapper, String sql,
                                                        InputParameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            DefaultAutoCloseableScope resultSetAndStatementScope = connectionScope.add(new DefaultAutoCloseableScope(), DefaultAutoCloseableScope::close);
            try (DefaultAutoCloseableScope executeQueryScope = new DefaultAutoCloseableScope()) {
                PreparedStatement statement = resultSetAndStatementScope.add(connection.prepareStatement(sql), PreparedStatement::close);
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
                 DefaultAutoCloseableScope statementScope = new DefaultAutoCloseableScope()) {
                bindCallableParameters(parameters, statement, statementScope);
                statement.execute();
            }
        });
    }

    @Override
    public <MappedRowType> Stream<MappedRowType> call(Function<RowType, MappedRowType> rowMapper, String sql,
                                                      Parameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
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

    private <MapperRowType> Stream<MapperRowType> executeQuery(Function<RowType, MapperRowType> rowMapper,
                                                               DefaultAutoCloseableScope resultSetAndStatementScope,
                                                               PreparedStatement statement) throws SQLException {
        ResultSet resultSet = resultSetAndStatementScope.add(statement.executeQuery(), ResultSet::close);
        return StreamSupport.stream(new RowSpliterator<>(resultSet, rowFactory),
                false)
                .onClose(resultSetAndStatementScope::close)
                .map(rowMapper);
    }

}
