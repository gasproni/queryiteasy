package com.asprotunity.queryiteasy.acceptance_tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        HSQLQueriesTest.class,
        HSQLSupportedTypesTest.class,
        OracleQueriesTest.class,
        OracleSupportedTypesTest.class
})
public class EndToEndSuite {
}
