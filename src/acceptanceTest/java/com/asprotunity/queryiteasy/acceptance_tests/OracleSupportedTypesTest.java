package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DefaultDataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import com.asprotunity.queryiteasy.io.StringIO;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class OracleSupportedTypesTest extends SupportedTypesTestCommon {

    private static DefaultDataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Oracle JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DefaultDataStore(dataSource);
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

    protected DefaultDataStore getDataStore() {
        return dataStore;
    }

    @Test
    public void stores_and_reads_longs_as_numbers() throws SQLException {
        Long value = 10L;
        List<Tuple2<Long, Long>> expectedValues = storeAndReadValuesBack("NUMBER", Row::asLong, bind((Long) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_times_as_sql_dates() throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Tuple2<Time, Time>> expectedValues = storeAndReadValuesBack("DATE", Row::asTime, bind((Time) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_sql_timestamps_as_dates() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Tuple2<Timestamp, Timestamp>> expectedValues = storeAndReadValuesBack("DATE", Row::asTimestamp, bind((Timestamp) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void doesnt_support_booleans() throws SQLException {
        try {
            storeAndReadValuesBack("BOOLEAN", Row::asBoolean, bind((Boolean) null), bind(true));
        } catch (RuntimeSQLException exc) {
            assertThat(exc.getCause().getMessage(), is("ORA-00902: invalid datatype\n"));
        }
    }

    @Test
    public void stores_and_reads_blobs() throws SQLException, UnsupportedEncodingException {
        String blobContent = "this is the content of the blob";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> inputStreamSupplier = () -> new ByteArrayInputStream(blobContent.getBytes(charset));
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first BLOB NULL, second BLOB NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bindBlob(() -> null), bindBlob(inputStreamSupplier));
        });

        Function<InputStream, String> blobReader = inputStream -> StringIO.readFrom(inputStream, charset);

        List<Tuple2> expectedValues = getDataStore().executeWithResult(connection ->
                connection.select(row -> new Tuple2<>(row.fromBlob(1, blobReader), row.fromBlob(2, blobReader)),
                        "SELECT * FROM testtable").collect(toList()));

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(blobContent));
    }


    @Test
    public void stores_and_reads_clobs() throws SQLException, UnsupportedEncodingException {
        String clobContent = "this is the content of the blob";
        Supplier<Reader> readerSupplier = () -> new StringReader(clobContent);

        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first CLOB NULL, second CLOB NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bindClob(() -> null), bindClob(readerSupplier));
        });


        List<Tuple2> expectedValues = getDataStore().executeWithResult(connection ->
                connection.select(row -> new Tuple2<>(row.fromClob(1, StringIO::readFrom),
                                row.fromClob(2, StringIO::readFrom)),
                        "SELECT * FROM testtable").collect(toList()));

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(clobContent));
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropStatements = connection.select(row -> row.asString("dropStatements"),
                    "select 'drop '||object_type||' '|| object_name|| " +
                            "DECODE(OBJECT_TYPE,'TABLE',' CASCADE CONSTRAINTS','') as dropStatements from user_objects"
            ).collect(toList());
            for (String statement : dropStatements) {
                if (isNotDropOfSystemOrLobIndex(statement)) {
                    connection.update(statement);
                }
            }
        });
    }

}
