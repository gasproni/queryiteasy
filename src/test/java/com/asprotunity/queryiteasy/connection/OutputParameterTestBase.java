package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;
import org.junit.Before;

import java.sql.CallableStatement;

import static org.mockito.Mockito.mock;

public class OutputParameterTestBase {

    protected CallableStatement statement;
    protected Scope statementScope;
    protected int position;

    @Before
    public void setUp() throws Exception {
        statement = mock(CallableStatement.class);
        statementScope = new Scope();
        position = 1;
    }

    protected <T> void bindParameterAndMakeCall(OutputParameter<T> outputParameter) {
        outputParameter.bind(statement, position, statementScope);
        statementScope.close();
    }
}
