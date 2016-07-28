package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.DefaultAutoCloseableScope;

import java.sql.CallableStatement;

import static org.mockito.Mockito.mock;

public class OutputParameterTestBase {

    protected CallableStatement statement = mock(CallableStatement.class);
    protected int position = 1;
    private DefaultAutoCloseableScope queryScope = new DefaultAutoCloseableScope();

    protected void bindParameterAndEmulateCall(Parameter parameter) {
        parameter.bind(statement, position, queryScope);
        queryScope.close();
    }
}
