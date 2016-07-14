package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.SQLDataConverters;
import com.asprotunity.queryiteasy.scope.AutoCloseableScope;
import com.asprotunity.queryiteasy.scope.Scope;
import org.junit.Test;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowFromResultSetTest {

    @Test
    public void column_names_are_normalised_correctly_at_creation() throws SQLException {

        String mixedCaseColumnName = "ThisIsAColumnName";
        assertThat(mixedCaseColumnName, not(RowFromResultSet.normaliseColumnLabel(mixedCaseColumnName)));

        ResultSetWrapper rs = mock(ResultSetWrapper.class);
        when(rs.columnCount()).thenReturn(1);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.columnLabel(1)).thenReturn(mixedCaseColumnName);

        Scope dummyScope = new AutoCloseableScope();
        RowFromResultSet rowFromResultSet = new RowFromResultSet(rs);

        assertThat(SQLDataConverters.asInteger(rowFromResultSet.at(mixedCaseColumnName.toLowerCase())), is(1));

    }

}