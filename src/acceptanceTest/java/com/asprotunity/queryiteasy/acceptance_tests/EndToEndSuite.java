package com.asprotunity.queryiteasy.acceptance_tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        QueriesTest.class,
        SupportedTypesTest.class,
})
public class EndToEndSuite {
}
