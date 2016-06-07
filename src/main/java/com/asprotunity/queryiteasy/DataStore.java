package com.asprotunity.queryiteasy;

import com.asprotunity.queryiteasy.connection.Connection;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.internal.connection.ResultSetWrapperFactory;
import com.asprotunity.queryiteasy.internal.connection.WrappedJDBCConnection;
import com.asprotunity.queryiteasy.internal.connection.WrappedJDBCResultSet;

import javax.sql.DataSource;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataStore {

    private final ResultSetWrapperFactory resultSetWrapperFactory;
    private DataSource dataSource;

    public DataStore(DataSource dataSource) {
        if (dataSource == null) {
            throw new InvalidArgumentException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
        resultSetWrapperFactory = WrappedJDBCResultSet::new;
    }

    public void execute(Consumer<Connection> transaction) {
        RuntimeSQLException.execute(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection(),
                            resultSetWrapperFactory)) {
                        transaction.accept(connection);
                        connection.commit();
                    }
                }
        );
    }

    public <ResultType> ResultType executeWithResult(Function<Connection, ResultType> transaction) {
        return RuntimeSQLException.executeAndReturnResult(() -> {
                    try (WrappedJDBCConnection connection = new WrappedJDBCConnection(dataSource.getConnection(),
                            resultSetWrapperFactory)) {
                        ResultType result = transaction.apply(connection);
                        connection.commit();
                        return result;
                    }
                }
        );
    }

}
