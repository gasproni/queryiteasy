package com.asprotunity.queryiteasy;

import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RowFactory;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.internal.connection.WrappedJDBCConnection;

import javax.sql.DataSource;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataStore<RowType extends Row> {

    private DataSource dataSource;
    private RowFactory<RowType> rowFactory;

    public DataStore(DataSource dataSource, RowFactory<RowType> rowFactory) {
        if (dataSource == null) {
            throw new InvalidArgumentException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
        this.rowFactory = rowFactory;
    }

    public void execute(Consumer<Connection<RowType>> transaction) {
        RuntimeSQLException.execute(() -> {
                    try (WrappedJDBCConnection<RowType> connection =
                                 new WrappedJDBCConnection<>(dataSource.getConnection(), rowFactory)) {
                        transaction.accept(connection);
                        connection.commit();
                    }
                }
        );
    }

    public <ResultType> ResultType executeWithResult(Function<Connection<RowType>, ResultType> transaction) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
                    try (WrappedJDBCConnection<RowType> connection =
                                 new WrappedJDBCConnection<>(dataSource.getConnection(), rowFactory)) {
                        ResultType result = transaction.apply(connection);
                        connection.commit();
                        return result;
                    }
                }
        );
    }
}
