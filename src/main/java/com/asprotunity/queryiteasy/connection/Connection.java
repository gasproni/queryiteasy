package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {

    /**
     * Executes the given sql command. This call is for updating / inserting data in the db.
     * @param sql The SQL code to execute. It may contain positional parameters (denoted by question marks).
     * @param parameters The values to bind to the positional parameters in the {@code sql} parameter.
     * @throws InvalidArgumentException if {@code sql == null || sql.isEmpty()} or {@code parameters == null}.
     * @throws RuntimeSQLException If a {@link java.sql.SQLException} is thrown during the call.
     */
    void update(String sql, InputParameter... parameters);

    /**
     * Executes the given batch updates. This call is for updating / inserting data in the db.
     * @param sql The SQL code to execute. It must have positional parameters (denoted by question marks) to be bound
     *            with values for each batch.
     * @param batches Batches of values to bind to the positional parameters in the {@code sql} parameter.
     * @throws InvalidArgumentException if any of the arguments is null or empty.
     * @throws RuntimeSQLException If a {@link java.sql.SQLException} is thrown during the call.
     */
    void update(String sql, List<Batch> batches);

    <MappedRowType> Stream<MappedRowType> select(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                                 InputParameter... parameters);

    void call(String sql, Parameter... parameters);

    <MappedRowType> Stream<MappedRowType> call(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                               Parameter... parameters);
}
