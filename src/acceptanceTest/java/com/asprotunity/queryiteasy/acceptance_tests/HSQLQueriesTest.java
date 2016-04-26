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

import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class HSQLQueriesTest extends QueriesTestBase {


    private static DataSource dataSource;

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        dataSource = DataSourceInstantiator.configureHSQLInMemoryDataSource();
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

        prepareExpectedData("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
        prepareExpectedData("CREATE PROCEDURE insert_new_record(in first INTEGER, inout ioparam  VARCHAR(20)," +
                "                                               in other VARCHAR(20), out res VARCHAR(20))\n" +
                "MODIFIES SQL DATA\n" +
                "BEGIN ATOMIC \n" +
                "   INSERT INTO testtable VALUES (first, other);\n" +
                "   SET res = ioparam;\n" +
                "   SET ioparam = 'NewString';\n" +
                " END");


        StringInputOutputParameter inputOutputParameter = new StringInputOutputParameter("OldString");
        StringOutputParameter outputParameter = new StringOutputParameter();
        getDataStore().execute(connection ->
                connection.call("{call insert_new_record(?, ?, ?, ?)}", bind(10), inputOutputParameter,
                        bind("asecond10"), outputParameter)
        );

        List<Row> expectedValues = query("SELECT * FROM testtable");

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
        assertThat(expectedValues.get(0).asString("second"), is("asecond10"));
        assertThat(inputOutputParameter.value(), is("NewString"));
        assertThat(outputParameter.value(), is("OldString"));
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> connection.update("DROP SCHEMA PUBLIC CASCADE"));
    }

}
