package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.scope.AutoCloseableScope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.Types;

import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class InputParameterBindersTest {

    private PreparedStatement preparedStatement;
    private int position;
    private AutoCloseableScope scope;

    @Before
    public void setUp() {
        preparedStatement = mock(PreparedStatement.class);
        position = 1;
        scope = new AutoCloseableScope();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void binds_strings_correctly() throws Exception {
        String value = "astring";
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setString(position, value);
    }

    @Test
    public void binds_valid_shorts_correctly() throws Exception {
        short value = 10;
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, value, Types.SMALLINT);
    }

    @Test
    public void binds_null_shorts_correctly() throws Exception {
        bind((Short) null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, null, Types.SMALLINT);
    }

    @Test
    public void binds_valid_integers_correctly() throws Exception {
        int value = 10;
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, value, Types.INTEGER);
    }

    @Test
    public void binds_null_integers_correctly() throws Exception {
        bind((Integer) null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, null, Types.INTEGER);
    }

    @Test
    public void binds_valid_longs_correctly() throws Exception {
        long value = 10;
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, value, Types.BIGINT);
    }

    @Test
    public void binds_null_longs_correctly() throws Exception {
        bind((Long) null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, null, Types.BIGINT);
    }

    @Test
    public void binds_valid_doubles_correctly() throws Exception {
        double value = 10;
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, value, Types.DOUBLE);
    }

    @Test
    public void binds_null_doubles_correctly() throws Exception {
        bind((Double) null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, null, Types.DOUBLE);
    }

    @Test
    public void binds_valid_floats_correctly() throws Exception {
        float value = 10;
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, value, Types.REAL);
    }

    @Test
    public void binds_null_floats_correctly() throws Exception {
        bind((Float) null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, null, Types.REAL);
    }

    @Test
    public void binds_valid_bytes_correctly() throws Exception {
        byte value = 10;
        bind(value).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, value, Types.TINYINT);
    }

    @Test
    public void binds_null_bytes_correctly() throws Exception {
        bind((Byte) null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setObject(position, null, Types.TINYINT);
    }

    @Test
    public void binds_valid_blobs_correctly() throws Exception {
        InputStream blobStream = mock(InputStream.class);
        InputParameterBinders.bindBlob(() -> blobStream).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setBlob(position, blobStream);
        assertThat(this.scope.handlersCount(), is(1));
        assertThatStreamOnLeaveRegisteredCorrectly(scope, blobStream);

    }

    @Test
    public void binds_null_blobs_correctly() throws Exception {
        InputParameterBinders.bindBlob(() -> null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setNull(position, Types.BLOB);
        assertThat(scope.handlersCount(), is(0));
    }

    @Test
    public void binds_valid_clobs_correctly() throws Exception {
        Reader clobReader = mock(Reader.class);
        InputParameterBinders.bindClob(() -> clobReader).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setClob(position, clobReader);
        assertThat(this.scope.handlersCount(), is(1));
        assertThatReaderOnLeaveRegisteredCorrectly(scope, clobReader);

    }

    @Test
    public void binds_null_clobs_correctly() throws Exception {
        InputParameterBinders.bindClob(() -> null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setNull(position, Types.CLOB);
        assertThat(scope.handlersCount(), is(0));
    }

    @Test
    public void binds_valid_longvarbinaries_correctly() throws Exception {
        InputStream binaryStream = mock(InputStream.class);
        InputParameterBinders.bindLongVarbinary(() -> binaryStream).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setBinaryStream(position, binaryStream);
        assertThat(this.scope.handlersCount(), is(1));
        assertThatStreamOnLeaveRegisteredCorrectly(scope, binaryStream);

    }

    @Test
    public void binds_null_longvarbinaries_correctly() throws Exception {
        InputParameterBinders.bindLongVarbinary(() -> null).bind(preparedStatement, position, scope);
        verify(preparedStatement, times(1)).setNull(position, Types.LONGVARBINARY);
        assertThat(scope.handlersCount(), is(0));
    }

    private void assertThatStreamOnLeaveRegisteredCorrectly(AutoCloseableScope scope, InputStream blobStream) throws IOException {
        verify(blobStream, times(0)).close();
        scope.close();
        verify(blobStream, times(1)).close();
    }

    private void assertThatReaderOnLeaveRegisteredCorrectly(AutoCloseableScope scope, Reader blobReader) throws IOException {
        verify(blobReader, times(0)).close();
        scope.close();
        verify(blobReader, times(1)).close();
    }
}