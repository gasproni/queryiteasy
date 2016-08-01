package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;


/**
 * This is the interface that all input parameters must implement.
 */
@FunctionalInterface
public interface InputParameter extends Parameter {

    /**
     * This is the method to implement to bind a parameter of a Java type to a SQL parameter in the database.
     * All the inputs to the method are passed by the framework and are guaranteed not to be null.
     *
     * Here is an example of usage:
     * <pre>
     * {@code
     *   public static InputParameter bindBlob(Supplier<InputStream> streamSupplier) {
     *       InvalidArgumentException.throwIfNull(streamSupplier, "streamSupplier");
     *       return (statement, position, queryScope) -> {
     *           InputStream inputStream = streamSupplier.get();
     *           RuntimeSQLException.execute(() -> {
     *               if (inputStream == null) {
     *                   statement.setNull(position, Types.BLOB);
     *               } else {
     *                   queryScope.add(inputStream::close);
     *                   statement.setBlob(position, inputStream);
     *               }
     *           });
     *       };
     *   }
     * }
     * </pre>
     * In the call above, the queryScope parameter is used to close the inputStream after it has been read during
     * the query.
     *
     * @param statement The PreparedStatement instance passed by the framework.
     * @param position The position of the parameter passed by the framework. Starts at 1.
     * @param queryScope The scope inside which the call to execute() or executeQuery() will happen.
     */
    void bind(PreparedStatement statement, int position, Scope queryScope);

    /**
     * Do not reimplement this method. The default is necessary to make the InputParameter
     * work correctly and keep the interface "functional".
     */
    @Override
    default void bind(CallableStatement statement, int position, Scope queryScope) {
        bind((PreparedStatement) statement, position, queryScope);
    }

}
