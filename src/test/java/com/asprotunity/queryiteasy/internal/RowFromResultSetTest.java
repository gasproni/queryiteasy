package com.asprotunity.queryiteasy.internal;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowFromResultSetTest {

    @Test
    public void column_names_are_normalised_correctly_at_creation() throws SQLException {

        String mixedCaseColumnName = "ThisIsAColumnName";
        assertThat(mixedCaseColumnName, not(RowFromResultSet.normaliseColumnName(mixedCaseColumnName)));

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData rsm = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(rsm);
        when(rsm.getColumnCount()).thenReturn(1);
        when(rsm.getColumnLabel(1)).thenReturn(mixedCaseColumnName);
        when(rs.getObject(1)).thenReturn(1);

        RowFromResultSet rowFromResultSet = new RowFromResultSet(rs);

        assertThat(rowFromResultSet.asInteger(mixedCaseColumnName.toLowerCase()), is(1));

    }

}