package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.StringInputOutputParameter;
import com.asprotunity.queryiteasy.connection.StringOutputParameter;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

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

        prepareExpectedData("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
        prepareExpectedData("CREATE PROCEDURE insert_new_record(first in INTEGER, ioparam in out VARCHAR," +
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

        List<Row> expectedValues = query("SELECT * FROM testtable");

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
        assertThat(expectedValues.get(0).asString("second"), is("asecond10"));
        assertThat(inputOutputParameter.value(), is("NewString"));
        assertThat(outputParameter.value(), is("OldString"));
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropStatements = connection.select("select 'drop '||object_type||' '|| object_name|| " +
                            "DECODE(OBJECT_TYPE,'TABLE',' CASCADE CONSTRAINTS','') as command from user_objects",
                    rowStream -> rowStream.map(row -> row.asString("command")).collect(Collectors.toList()));
            for (String statement : dropStatements) {
                connection.update(statement);
            }
        });
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = Paths.get("test_datasources", "oracle.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = PropertiesLoader.loadProperties(path);

        DataSource result = DataSourceInstantiator.instantiateDataSource(properties.getProperty("queryiteasy.oracle.datasource.class"));

        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.url"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.password"));
        return result;

    }

}
