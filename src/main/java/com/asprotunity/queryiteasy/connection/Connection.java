package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Connection {

    /**
     * Executes the given SQL statements. This call is for updating / inserting data in the db.
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

    /**
     * Executes the given SQL query.
     * @param rowMapper Maps a result set into a caller provided type.
     * @param sql The SQL code to execute. It may contain positional parameters (denoted by question marks).
     * @param parameters The values to bind to the positional parameters in the {@code sql} parameter.
     * @param <MappedRowType> A type provided by the caller.
     * @return A lazy {@link java.util.stream.Stream} containing the results of {@code rowMapper} applied to each result
     *        in the {@link java.sql.ResultSet} returned by the underlying JDBC query.
     *        The stream must be consumed inside the transaction in which the select has been called.
     * @throws InvalidArgumentException if any of the arguments is null or if {@code sql} is empty.
     * @throws RuntimeSQLException If a {@link java.sql.SQLException} is thrown during the call.
     */
    <MappedRowType> Stream<MappedRowType> select(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                                 InputParameter... parameters);

    /**
     * Executes the given stored function or procedure call. It support the JDBC escape syntax {@code "{call proc_name(?, ?)}"}.
     * It supports also the {@code "{? = call proc_name(?, ?)}"} form, provided the underlying JDBC driver supports it, in which case,
     * the first parameter must be an {@code OutputParameter} of the type returned by the stored procedure.
     * After the call, the input-output and output parameters passed to it will contain the new values set by the stored procedure / function.
     * @param sql The call code to execute. It may contain positional parameters (denoted by question marks).
     * @param parameters The values to bind to the positional parameters in the {@code sql} parameter.
     * @throws InvalidArgumentException if any of the arguments is null or if {@code sql} is empty.
     * @throws RuntimeSQLException If a {@link java.sql.SQLException} is thrown during the call.
     */
    void call(String sql, Parameter... parameters);

    /**
     * Executes the given stored function or procedure call. It support the JDBC escape syntax {@code "{call proc_name(?, ?)}"}.
     * After the call, the input-output and output parameters passed to it will contain the new values set by the stored procedure / function.
     * @param rowMapper Maps a result set into a caller provided type.
     * @param sql The SQL code to execute. It may contain positional parameters (denoted by question marks).
     * @param parameters The values to bind to the positional parameters in the {@code sql} parameter.
     * @param <MappedRowType> A type provided by the caller.
     * @return A lazy {@link java.util.stream.Stream} containing the results of {@code rowMapper} applied to each result
     *        in the {@link java.sql.ResultSet} returned by the underlying JDBC query.
     *        The stream must be consumed inside the transaction in which the select has been called.
     * @throws InvalidArgumentException if any of the arguments is null or if {@code sql} is empty.
     * @throws RuntimeSQLException If a {@link java.sql.SQLException} is thrown during the call.
     */
    <MappedRowType> Stream<MappedRowType> call(Function<ResultSet, MappedRowType> rowMapper, String sql,
                                               Parameter... parameters);
}
