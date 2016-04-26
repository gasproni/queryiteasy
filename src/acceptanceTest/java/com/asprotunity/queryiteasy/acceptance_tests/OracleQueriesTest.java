package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.StringInputOutputParameter;
import com.asprotunity.queryiteasy.connection.StringOutputParameter;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

import static com.asprotunity.queryiteasy.acceptance_tests.OracleConfigurationAndSchemaDrop.configureDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.OracleConfigurationAndSchemaDrop.dropSchemaObjects;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class OracleQueriesTest extends QueriesTestBase {

    private static DataSource dataSource;

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        dataSource = configureDataSource();
        assumeTrue("No Oracle JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
    }

    @Override
    protected DataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
    }


    @Test
    public void calls_stored_procedure_with_bind_values() throws SQLException {

        DataSourceInstantiationAndAccess.prepareData(getDataSource(), "CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)",
                "CREATE PROCEDURE insert_new_record(first in INTEGER, ioparam in out VARCHAR," +
                        "                                               other in VARCHAR, res out VARCHAR)\n" +
                        "IS\n" +
                        "BEGIN\n" +
                        "   INSERT INTO testtable VALUES (first, other);\n" +
                        "   res := ioparam;\n" +
                        "   ioparam := 'NewString';\n" +
                        " END;");


        StringInputOutputParameter inputOutputParameter = new StringInputOutputParameter("OldString");
        StringOutputParameter outputParameter = new StringOutputParameter();
        getDataStore().execute(connection ->
                connection.call("{call insert_new_record(?, ?, ?, ?)}", bind(10), inputOutputParameter,
                        bind("asecond10"), outputParameter)
        );

        List<Row> expectedValues = DataSourceInstantiationAndAccess.query(getDataSource(), "SELECT * FROM testtable");

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
        assertThat(expectedValues.get(0).asString("second"), is("asecond10"));
        assertThat(inputOutputParameter.value(), is("NewString"));
        assertThat(outputParameter.value(), is("OldString"));
    }

    @Override
    protected void cleanup() throws Exception {
        dropSchemaObjects(getDataStore());
    }

}
