package com.asprotunity.queryiteasy.exception;

import org.junit.Test;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class RuntimeSQLExceptionTest {

    @Test
    public void wraps_SQLException_with_RuntimeSQLException() {

        assertWrapsInsideRuntimeSQLException(new SQLException(),
                                             exception -> RuntimeSQLException.execute(() -> {throw exception;}));

        assertWrapsInsideRuntimeSQLException(new SQLException(),
                                             exception -> RuntimeSQLException.executeWithResult(() -> {
                                                 throw exception;
                                             }));

    }

    private void assertWrapsInsideRuntimeSQLException(SQLException exception, Consumer<SQLException> consumer) {
        try {
            consumer.accept(exception);
            fail("Exception expected.");
        } catch (RuntimeSQLException e) {
            assertThat(e.getCause(), is(exception));
        }
    }

}