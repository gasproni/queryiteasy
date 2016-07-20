package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.connection.BlobReaders.fromClob;

public class ClobOutputParameter<ResultType> implements OutputParameter {

    private Function<Reader, ResultType> clobReader;
    private ResultType value = null;

    public ClobOutputParameter(Function<Reader, ResultType> clobReader) {
        if (clobReader == null) {
            throw new InvalidArgumentException("clobReader cannot be null");
        }
        this.clobReader = clobReader;
    }

    public ResultType value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.CLOB);
            statementScope.add(() -> value = fromClob(statement.getClob(position), clobReader));
        });
    }
}
