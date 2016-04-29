package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class OracleSupportedTypesTest extends SupportedTypesTestCommon {

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Oracle JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
    }

    protected DataStore getDataStore() {
        return dataStore;
    }

    @Test
    public void stores_and_reads_longs_as_numbers() throws SQLException {
        Long value = 10L;
        List<Row> expectedValues = storeAndReadValuesBack("NUMBER", bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asLong("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asLong("second"), is(value));
    }

    @Test
    public void stores_and_reads_times_as_sql_dates() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asTime("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asTime("second"), is(value));
    }

    @Test
    public void stores_and_reads_sql_timestamps_as_dates() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDate("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDate("second"), is(value));
    }


    @Test
    public void doesnt_support_booleans() throws SQLException {
        try {
            storeAndReadValuesBack("BOOLEAN", bind((Boolean) null), bind(true));
        } catch (RuntimeSQLException exc) {
            assertThat(exc.getCause().getMessage(), is("ORA-00902: invalid datatype\n"));
        }
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropStatements = connection.select("select 'drop '||object_type||' '|| object_name|| " +
                            "DECODE(OBJECT_TYPE,'TABLE',' CASCADE CONSTRAINTS','') as dropStatements from user_objects",
                    rowStream -> rowStream.map(row -> row.asString("dropStatements")).collect(Collectors.toList()));
            for (String statement : dropStatements) {
                if (isNotDropOfSystemOrLobIndex(statement)) {
                    connection.update(statement);
                }
            }
        });
    }

    private static boolean isNotDropOfSystemOrLobIndex(String statement) {
        String statementToLower = statement.toLowerCase();
        return !statementToLower.startsWith("drop index sys_") && !statementToLower.startsWith("drop lob");
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("oracle.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = TestPropertiesLoader.loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.oracle.datasource.class"));

        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.url"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.oracle.datasource.password"));
        return result;

    }

}
