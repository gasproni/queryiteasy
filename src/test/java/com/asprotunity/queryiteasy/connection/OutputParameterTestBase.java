package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.Scope;
import org.junit.Before;

import java.sql.CallableStatement;

import static org.mockito.Mockito.mock;

public class OutputParameterTestBase {

    protected CallableStatement statement;
    protected int position;
    private Scope statementScope;

    @Before
    public void setUp() throws Exception {
        statement = mock(CallableStatement.class);
        statementScope = new Scope();
        position = 1;
    }

    protected void bindParameterAndMakeCall(Parameter parameter) {
        parameter.bind(statement, position, statementScope);
        statementScope.close();
    }
}
