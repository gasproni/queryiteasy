package com.asprotunity.tersql.connection;

import com.asprotunity.tersql.exception.InvalidArgumentException;
import org.junit.Test;

import static com.asprotunity.tersql.connection.StatementParameter.bind;

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