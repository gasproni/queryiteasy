package com.asprotunity.queryiteasy.scope;

import org.junit.Test;
import org.mockito.InOrder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AutoCloseableScopeTest {


    @Test
    public void scope_is_not_closed_after_instatiation() {
        AutoCloseableScope scope = new AutoCloseableScope();
        assertFalse(scope.isClosed());
    }

    @Test
    public void closes_scope_correctly_when_no_handlers_registered() {
        AutoCloseableScope scope = new AutoCloseableScope();
        scope.close();
        assertTrue(scope.isClosed());
    }

    @Test
    public void calls_single_registered_close_handler() throws Exception {
        Closeable closeable = mock(Closeable.class);

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(closeable::close);
        scope.close();

        verify(closeable, times(1)).close();
        assertTrue(scope.isClosed());

    }

    @Test
    public void close_calls_close_on_close_handlers_only_once() throws Exception {
        Closeable closeable = mock(Closeable.class);

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(closeable::close);
        scope.close();
        scope.close();

        verify(closeable, times(1)).close();
        assertTrue(scope.isClosed());

    }

    @Test
    public void calls_registered_close_handlers_in_reverse_registration_order() throws Exception {
        Closeable closeable1 = mock(Closeable.class);
        Closeable closeable2 = mock(Closeable.class);

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(closeable1::close);
        scope.add(closeable2::close);
        scope.close();

        InOrder order = inOrder(closeable1, closeable2);
        order.verify(closeable2, times(1)).close();
        order.verify(closeable1, times(1)).close();
        assertTrue(scope.isClosed());
    }

    @Test
    public void closes_normally_and_throws_runtime_exception_when_handler_throws() throws Exception {
        Exception exceptionFromCloseable = new Exception();
        Closeable closeable = mock(Closeable.class);
        doThrow(exceptionFromCloseable).when(closeable).close();

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(closeable::close);

        try {
            scope.close();
            fail("DisposerExceptionExpected!");
        } catch (ScopeException exception) {
            assertTrue(scope.isClosed());
            assertThat(exception.getCause(), is(exceptionFromCloseable));
        }
    }

    @Test
    public void calls_all_handlers_and_throws_runtime_exception_wrapping_first_thrown_with_all_others_marked_as_suppressed_when_more_than_one_handler_throws() throws Exception {
        Closeable closeable1 = mock(Closeable.class);
        Exception closeable1Exception = new Exception();
        doThrow(closeable1Exception).when(closeable1).close();

        Closeable closeable2 = mock(Closeable.class);

        Closeable closeable3 = mock(Closeable.class);
        Exception closeable3Exception = new Exception();
        doThrow(closeable3Exception).when(closeable3).close();

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(closeable1::close);
        scope.add(closeable2::close);
        scope.add(closeable3::close);

        try {
            scope.close();
            fail("DisposerExceptionExpected!");
        } catch (ScopeException exception) {
            assertTrue(scope.isClosed());
            assertThat(exception.getCause(), is(closeable3Exception));
            assertThat(exception.getSuppressed().length, is(1));
            assertThat(exception.getSuppressed()[0], is(closeable1Exception));
            verify(closeable1, times(1)).close();
            verify(closeable2, times(1)).close();
            verify(closeable3, times(1)).close();
        }
    }

    private interface Closeable {
        void close() throws Exception;
    }

}