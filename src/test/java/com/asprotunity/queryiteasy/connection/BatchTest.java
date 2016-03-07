package com.asprotunity.queryiteasy.connection;

import com.asprotunity.queryiteasy.exception.InvalidArgumentException;
import org.junit.Test;

import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;

public class BatchTest {

    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_first_parameter_is_null() {
        Batch.batch(null);
    }

    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_other_parameters_array_is_null() {
        Batch.batch(InputParameterDefaultBinders.bind("astring"), (InputParameter[])null);
    }


    @Test(expected = InvalidArgumentException.class)
    public void throws_exception_when_a_parameter_in_the_array_is_null() {
        Batch.batch(InputParameterDefaultBinders.bind("astring"), (InputParameter)null);
    }

}