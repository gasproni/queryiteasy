package com.asprotunity.queryiteasy.scope;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class AutoCloseableScopeTest {

    private static class Disposable {
        private Exception exception;
        public int disposeOrder = -1;
        private static int disposeOrderCounter = 1;

        public Disposable() {
        }

        public Disposable(Exception exception) {
            this.exception = exception;
        }

        public void dispose() throws Exception {
            if (exception != null) {
                throw exception;
            }
            disposeOrder = disposeOrderCounter;
            ++disposeOrderCounter;
        }

        public boolean isDisposed() {
            return disposeOrder > 0;
        }
    }

    @Test
    public void calls_single_registered_close_handler() {
        Disposable disposable = new Disposable();
        assertFalse(disposable.isDisposed());

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(disposable::dispose);
        scope.close();

        assertTrue(disposable.isDisposed());
    }

    @Test
    public void calls_registered_close_handlers_in_reverse_registration_order() {
        Disposable disposable1 = new Disposable();
        Disposable disposable2 = new Disposable();

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(disposable1::dispose);
        scope.add(disposable2::dispose);
        scope.close();

        assertThat(disposable1.disposeOrder, is(greaterThan(disposable2.disposeOrder)));
    }

    @Test
    public void works_correctly_when_no_handlers_registered() {
        AutoCloseableScope scope = new AutoCloseableScope();
        assertFalse(scope.isClosed());
        scope.close();
        assertTrue(scope.isClosed());
    }


    @Test
    public void closes_normally_and_throws_runtime_exception_when_handler_throws() {
        Exception thrownByDisposable = new Exception();
        Disposable disposable = new Disposable(thrownByDisposable);

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(disposable::dispose);

        try {
            scope.close();
            fail("DisposerExceptionExpected!");
        }
        catch (ScopeException exception) {
            assertTrue(scope.isClosed());
            assertThat(exception.getCause(), is(thrownByDisposable));
        }
    }

    @Test
    public void calls_all_handlers_and_throws_runtime_exception_wrapping_first_thrown_with_all_others_marked_as_suppressed_when_more_than_one_handler_throws() {
        Exception disposable1Exception = new Exception();
        Disposable disposable1 = new Disposable(disposable1Exception);

        Disposable nonThrowingDisposable = new Disposable();

        Exception disposable2Exception = new Exception();
        Disposable disposable2 = new Disposable(disposable2Exception);

        AutoCloseableScope scope = new AutoCloseableScope();
        scope.add(disposable1::dispose);
        scope.add(nonThrowingDisposable::dispose);
        scope.add(disposable2::dispose);

        try {
            scope.close();
            fail("DisposerExceptionExpected!");
        }
        catch (ScopeException exception) {
            assertTrue(scope.isClosed());
            assertThat(exception.getCause(), is(disposable2Exception));
            assertThat(exception.getSuppressed().length, is(1));
            assertThat(exception.getSuppressed()[0], is(disposable1Exception));
            assertTrue(nonThrowingDisposable.isDisposed());
        }
    }
}