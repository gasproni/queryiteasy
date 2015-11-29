package com.asprotunity.jdbcunboil.connection;

import com.asprotunity.jdbcunboil.exception.InvalidArgumentException;
import org.junit.Test;

import static com.asprotunity.jdbcunboil.connection.StatementParameter.bind;
import static org.junit.Assert.*;

public class BatchTest {

    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_first_parameter_is_null() {
        Batch.batch(null);
    }

    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_other_parameters_array_is_null() {
        Batch.batch(bind("astring"), (StatementParameter[])null);
    }


    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_a_parameter_in_the_array_is_null() {
        Batch.batch(bind("astring"), (StatementParameter)null);
    }

}