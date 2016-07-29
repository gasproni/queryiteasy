package com.asprotunity.queryiteasy.exception;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class InvalidArgumentExceptionTest {

    @Test
    public void throws_when_expression_is_true() {
        try {
            InvalidArgumentException.throwIf(true, "message");
            fail("Exception expected.");
        } catch (InvalidArgumentException exception) {
            assertThat(exception.getMessage(), is("message"));
        }
    }

    @Test
    public void doesnt_throw_when_expression_is_false() {
        InvalidArgumentException.throwIf(false, "doesn't matter");
    }

    @Test
    public void throws_when_parameter_is_null() {
        try {
            InvalidArgumentException.throwIfNull(null, "parameter");
            fail("Exception expected.");
        } catch (InvalidArgumentException exception) {
            assertThat(exception.getMessage(), is("parameter cannot be null."));
        }
    }

    @Test
    public void doesnt_throw_when_parameter_not_null() {
        InvalidArgumentException.throwIfNull(new Object(), "doesn't matter");
    }


}