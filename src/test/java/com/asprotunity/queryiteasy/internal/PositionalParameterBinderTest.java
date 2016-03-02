package com.asprotunity.queryiteasy.internal;

import com.asprotunity.queryiteasy.disposer.Disposer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.Types;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class PositionalParameterBinderTest {

    private PreparedStatement preparedStatement;
    private int position;
    private PositionalParameterBinder parameterBinder;
    private Disposer disposer;

    @Before
    public void setUp() {
        preparedStatement = mock(PreparedStatement.class);
        position = 1;
        disposer = Disposer.makeNew();
        parameterBinder = new PositionalParameterBinder(position, preparedStatement, disposer);
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
    public void binds_valid_shorts_correctly() throws Exception {
        short value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setObject(position, value, Types.SMALLINT);
    }

    @Test
    public void binds_null_shorts_correctly() throws Exception {
        parameterBinder.bind((Short) null);
        verify(preparedStatement, times(1)).setObject(position, null, Types.SMALLINT);
    }

    @Test
    public void binds_valid_integers_correctly() throws Exception {
        int value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setObject(position, value, Types.INTEGER);
    }

    @Test
    public void binds_null_integers_correctly() throws Exception {
        parameterBinder.bind((Integer) null);
        verify(preparedStatement, times(1)).setObject(position, null, Types.INTEGER);
    }

    @Test
    public void binds_valid_longs_correctly() throws Exception {
        long value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setObject(position, value, Types.BIGINT);
    }

    @Test
    public void binds_null_longs_correctly() throws Exception {
        parameterBinder.bind((Long) null);
        verify(preparedStatement, times(1)).setObject(position, null, Types.BIGINT);
    }

    @Test
    public void binds_valid_doubles_correctly() throws Exception {
        double value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setObject(position, value, Types.DOUBLE);
    }

    @Test
    public void binds_null_doubles_correctly() throws Exception {
        parameterBinder.bind((Double) null);
        verify(preparedStatement, times(1)).setObject(position, null, Types.DOUBLE);
    }

    @Test
    public void binds_valid_floats_correctly() throws Exception {
        float value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setObject(position, value, Types.REAL);
    }

    @Test
    public void binds_null_floats_correctly() throws Exception {
        parameterBinder.bind((Float) null);
        verify(preparedStatement, times(1)).setObject(position, null, Types.REAL);
    }

    @Test
    public void binds_valid_bytes_correctly() throws Exception {
        byte value = 10;
        parameterBinder.bind(value);
        verify(preparedStatement, times(1)).setObject(position, value, Types.TINYINT);
    }

    @Test
    public void binds_null_bytes_correctly() throws Exception {
        parameterBinder.bind((Byte) null);
        verify(preparedStatement, times(1)).setObject(position, null, Types.TINYINT);
    }

    @Test
    public void binds_valid_blobs_correctly() throws Exception {
        InputStream blobStream = mock(InputStream.class);
        parameterBinder.bind(() -> blobStream);
        verify(preparedStatement, times(1)).setBlob(position, blobStream);
        assertThat(this.disposer.handlersCount(), is(1));
        assertThatStreamCloseRegisteredCorrectly(disposer, blobStream);

    }

    @Test
    public void binds_null_blobs_correctly() throws Exception {
        parameterBinder.bind(() -> null);
        verify(preparedStatement, times(1)).setNull(position, Types.BLOB);
        assertThat(disposer.handlersCount(), is(0));
    }

    private void assertThatStreamCloseRegisteredCorrectly(Disposer disposer, InputStream blobStream) throws IOException {
        verify(blobStream, times(0)).close();
        disposer.close();
        verify(blobStream, times(1)).close();
    }
}