package com.asprotunity.queryiteasy.acceptance_tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        HSQLSupportedTypesTest.class,
        MySQLSupportedTypesTest.class,
        OracleSupportedTypesTest.class,
        QueriesTest.class
})
public class EndToEndSuite {
}
