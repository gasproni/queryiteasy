package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.*;
import com.asprotunity.queryiteasy.scope.AutoCloseableScope;
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
    private final ResultSetWrapperFactory resultSetWrapperFactory;

    public WrappedJDBCConnection(java.sql.Connection connection, ResultSetWrapperFactory resultSetWrapperFactory) {
        this.connection = connection;
        this.connectionScope = new AutoCloseableScope();
        this.resultSetWrapperFactory = resultSetWrapperFactory;
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
                 AutoCloseableScope scope = new AutoCloseableScope()) {
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
                 AutoCloseableScope statementScope = new AutoCloseableScope()) {
                for (Batch batch : batches) {
                    addBatch(batch, statement, statementScope);
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public <MappedRow> Stream<MappedRow> select(Function<Row, MappedRow> rowMapper, String sql,
                                                InputParameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            AutoCloseableScope scope = connectionScope.make(new AutoCloseableScope(), AutoCloseableScope::close);
            try (AutoCloseableScope executeQueryScope = new AutoCloseableScope()) {
                PreparedStatement statement = scope.make(connection.prepareStatement(sql), PreparedStatement::close);
                bindParameters(parameters, statement, executeQueryScope);
                return executeQuery(rowMapper, scope, statement);
            } catch (Exception ex) {
                scope.close();
                throw ex;
            }
        });
    }

    @Override
    public void call(String sql, Parameter... parameters) {
        RuntimeSQLException.execute(() -> {
            try (CallableStatement statement = connection.prepareCall(sql);
                 AutoCloseableScope statementScope = new AutoCloseableScope()) {
                bindCallableParameters(parameters, statement, statementScope);
                statement.execute();
            }
        });
    }

    @Override
    public <MappedRow> Stream<MappedRow> call(Function<Row, MappedRow> rowMapper, String sql, Parameter... parameters) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
            AutoCloseableScope scope = connectionScope.make(new AutoCloseableScope(), AutoCloseableScope::close);
            try (AutoCloseableScope executeQueryScope = new AutoCloseableScope()) {
                CallableStatement statement = scope.make(connection.prepareCall(sql), CallableStatement::close);
                bindCallableParameters(parameters, statement, executeQueryScope);
                return executeQuery(rowMapper, scope, statement);
            } catch (Exception ex) {
                scope.close();
                throw ex;
            }
        });
    }

    private <MappedRow> Stream<MappedRow> executeQuery(Function<Row, MappedRow> rowMapper,
                                                       AutoCloseableScope scope,
                                                       PreparedStatement statement) throws SQLException {
        ResultSet rs = scope.make(statement.executeQuery(), ResultSet::close);
        return StreamSupport.stream(new RowSpliterator(resultSetWrapperFactory.make(rs)),
                false)
                .onClose(scope::close)
                .map(rowMapper);
    }

}
