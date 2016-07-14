package com.asprotunity.queryiteasy.internal.connection;

import com.asprotunity.queryiteasy.connection.SQLDataConverters;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnLabel(1)).thenReturn(mixedCaseColumnName);

        ResultSet rs = mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(metaData);
        String value = "object value";
        when(rs.getObject(1)).thenReturn(value);

        RowFromResultSet rowFromResultSet = new RowFromResultSet(rs);

        assertThat(SQLDataConverters.asString(rowFromResultSet.at(mixedCaseColumnName.toLowerCase())), is(value));

    }

}