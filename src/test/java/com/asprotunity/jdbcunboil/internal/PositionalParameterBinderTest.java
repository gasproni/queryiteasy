package com.asprotunity.jdbcunboil.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.Types;

import static org.mockito.Mockito.*;

public class PositionalParameterBinderTest {

    private PreparedStatement preparedStatement;
    private int position;
    private PositionalParameterBinder parameterBinder;

    @Before
    public void setUp() {
        preparedStatement = mock(PreparedStatement.class);
        position = 1;
        parameterBinder = new PositionalParameterBinder(position, preparedStatement);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void binds_strings_correctly() throws Exception {
        String value = "astring";
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setString(position, value);
    }

    @Test
    public void binds_valid_integers_correctly() throws Exception {
        int value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setInt(position, value);
    }

    @Test
    public void binds_null_integers_correctly() throws Exception {
        parameterBinder.bind((Integer) null);
        verify(preparedStatement, times(1)).setNull(position, Types.INTEGER);
    }

    @Test
    public void binds_valid_doubles_correctly() throws Exception {
        double value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setDouble(position, value);
    }

    @Test
    public void binds_null_doubles_correctly() throws Exception {
        parameterBinder.bind((Double) null);
        verify(preparedStatement, times(1)).setNull(position, Types.DOUBLE);
    }

    @Test
    public void binds_valid_floats_correctly() throws Exception {
        float value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setFloat(position, value);
    }

    @Test
    public void binds_null_floats_correctly() throws Exception {
        parameterBinder.bind((Float) null);
        verify(preparedStatement, times(1)).setNull(position, Types.FLOAT);
    }

    @Test
    public void binds_valid_bytes_correctly() throws Exception {
        byte value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setByte(position, value);
    }

    @Test
    public void binds_null_bytes_correctly() throws Exception {
        parameterBinder.bind((Byte) null);
        verify(preparedStatement, times(1)).setNull(position, Types.TINYINT);
    }
}