package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import com.asprotunity.queryiteasy.scope.Scope;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.function.Function;

import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromLongVarbinary;

public class LongVarBinaryOutputParameter<ResultType> implements OutputParameter {

    private Function<InputStream, ResultType> longVarBinaryReader;
    private ResultType value = null;

    public LongVarBinaryOutputParameter(Function<InputStream, ResultType> longVarBinaryReader) {
        if (longVarBinaryReader == null) {
            throw new InvalidArgumentException("longVarBinaryReader cannot be null");
        }
        this.longVarBinaryReader = longVarBinaryReader;
    }

    public ResultType value() {
        return value;
    }

    @Override
    public void bind(CallableStatement statement, int position, Scope statementScope) {
        RuntimeSQLException.execute(() -> {
            statement.registerOutParameter(position, Types.LONGVARBINARY);
            statementScope.onLeave(() -> value = fromLongVarbinary(statement.getObject(position), longVarBinaryReader));
        });
    }
}
