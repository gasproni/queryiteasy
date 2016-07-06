package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.*;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;
import static com.asprotunity.queryiteasy.acceptance_tests.SupportedTypesTestCommon.readFrom;
import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class QueriesTest {

    private static DataSource dataSource;
    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        dataSource = HSQLInMemoryConfigurationAndSchemaDrop.configureHSQLInMemoryDataSource();
        dataStore = new DataStore(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        dropHSQLPublicSchema(dataStore);
    }

    @Test
    public void calls_stored_procedure_with_input_inputoutput_and_output_parameters() throws SQLException {

        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE PROCEDURE insert_new_record(in first INTEGER, inout ioparam  VARCHAR(20)," +
                "                                               in other VARCHAR(20), out res VARCHAR(20))\n" +
                "MODIFIES SQL DATA\n" +
                "BEGIN ATOMIC \n" +
                "   INSERT INTO testtable VALUES (first, other);\n" +
                "   SET res = ioparam;\n" +
                "   SET ioparam = 'NewString';\n" +
                " END");


        StringInputOutputParameter inputOutputParameter = new StringInputOutputParameter("OldString");
        StringOutputParameter outputParameter = new StringOutputParameter();
        dataStore.execute(connection ->
                connection.call("{call insert_new_record(?, ?, ?, ?)}", bind(10), inputOutputParameter,
                        bind("asecond10"), outputParameter)
        );

        List<Row> expectedValues = DataSourceInstantiationAndAccess.query(dataSource, "SELECT * FROM testtable");

        assertThat(expectedValues.size(), is(1));
        assertThat(asInteger(expectedValues.get(0).at("first")), is(10));
        assertThat(asString(expectedValues.get(0).at("second")), is("asecond10"));
        assertThat(inputOutputParameter.value(), is("NewString"));
        assertThat(outputParameter.value(), is("OldString"));
    }


    @Test
    public void calls_stored_procedure_with_blob_out_parameter() throws SQLException {

        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE PROCEDURE test_blob_out_param(in inparam BLOB, out outparam BLOB)\n" +
                "MODIFIES SQL DATA\n" +
                "BEGIN ATOMIC \n" +
                "   SET outparam = inparam;\n" +
                " END");

        String blobContent = "this is the content of the blob";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> inputBlobSupplier = () -> new ByteArrayInputStream(blobContent.getBytes(charset));

        BlobOutputParameter<String> outputParameter = new BlobOutputParameter<>(inputStream -> readFrom(inputStream, charset.name()));

        dataStore.execute(connection ->
                connection.call("{call test_blob_out_param(?, ?)}", bind(inputBlobSupplier), outputParameter)
        );

        assertThat(outputParameter.value(), is(blobContent));
    }

    @Test
    public void blob_input_out_parameter_works_correctly() throws SQLException {
        Charset charset = Charset.forName("UTF-8");

        String blobStoredInDb = "new blob value";
        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first BLOB NOT NULL)");
            connection.update("INSERT INTO testtable (first) VALUES (?)", bind(() -> new ByteArrayInputStream(blobStoredInDb.getBytes(charset))));

            connection.update("CREATE PROCEDURE test_blob_out_param(inout inoutparam BLOB, out outparam BLOB)\n" +
                    "MODIFIES SQL DATA\n" +
                    "BEGIN ATOMIC \n" +
                    "   SET outparam = inoutparam;\n" +
                    "   SELECT first into inoutparam from testtable;\n" +
                    " END");

        });

        String ioParamOriginalBlobContent = "this is the content of the blob";
        BlobInputOutputParameter<String> inputOutputParameter =
                new BlobInputOutputParameter<>(() -> new ByteArrayInputStream(ioParamOriginalBlobContent.getBytes(charset)),
                                               inputStream -> readFrom(inputStream, charset.name()));

        BlobOutputParameter<String> outputParameter = new BlobOutputParameter<>(inputStream -> readFrom(inputStream, charset.name()));

        dataStore.execute(connection ->
                connection.call("{call test_blob_out_param(?, ?)}", inputOutputParameter, outputParameter)
        );

        assertThat(outputParameter.value(), is(ioParamOriginalBlobContent));
        assertThat(inputOutputParameter.value(), is(blobStoredInDb));
    }


    @Test
    public void calls_function_with_no_input_parameters_and_returns_result_correctly() throws SQLException, ParseException {

        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE TABLE testtable (first INTEGER NOT NULL)");
        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE FUNCTION return_date()\n" +
                "RETURNS DATE\n" +
                "BEGIN ATOMIC \n" +
                "   RETURN TO_DATE('2016-06-23', 'YYYY-MM-DD');\n" +
                " END");

        Date result = dataStore.executeWithResult(connection ->
                connection.call("{call return_date()}", rowStream -> asDate(rowStream.findFirst().get().at(1)))
        );

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        assertThat(result, is(new Date(df.parse("2016-06-23").getTime())));
    }

    @Test
    public void inserts_with_no_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (first) VALUES (10)");
        });

        List<Row> expectedValues = DataSourceInstantiationAndAccess.query(dataSource, "SELECT first FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(asInteger(expectedValues.get(0).at("first")), is(10));
    }

    @Test
    public void rolls_back_transaction_when_exception_thrown() throws SQLException {

        try {
            dataStore.execute(connection -> {
                connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
                connection.update("INSERT INTO testtable (first) VALUES (10)");
                throw new RuntimeException();
            });
            fail("Exception expected");
        } catch (RuntimeException ex) {
            List<Row> expectedValues = DataSourceInstantiationAndAccess.query(dataSource, "SELECT first FROM testtable");
            assertThat(expectedValues.size(), is(0));
        }
    }

    @Test
    public void inserts_with_some_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(10), bind("asecond"));
        });

        List<Row> expectedValues = DataSourceInstantiationAndAccess.query(dataSource, "SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(asInteger(expectedValues.get(0).at("first")), is(10));
        assertThat(asString(expectedValues.get(0).at("second")), is("asecond"));
    }

    @Test
    public void does_batch_inserts() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    Arrays.asList(batch(bind(10), bind("asecond10")),
                            batch(bind(11), bind("asecond11")),
                            batch(bind(12), bind("asecond12"))));
        });

        List<Row> expectedValues = DataSourceInstantiationAndAccess.query(dataSource, "SELECT * FROM testtable ORDER BY first ASC");
        assertThat(expectedValues.size(), is(3));
        for (int index = 0; index < expectedValues.size(); ++index) {
            assertThat(asInteger(expectedValues.get(index).at("first")), is(index + 10));
            assertThat(asString(expectedValues.get(index).at("second")), is("asecond" + (index + 10)));
        }
    }

    @Test
    public void throws_exception_when_batch_is_empty() throws SQLException {

        try {
            dataStore.execute(connection -> {
                connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
                connection.update("INSERT INTO testtable (first, second) VALUES (1, 'sometext')",
                        Collections.emptyList());
            });
            fail("RuntimeSQLException expected!");
        } catch (RuntimeSQLException exception) {
            List<Row> expectedValues = DataSourceInstantiationAndAccess.query(dataSource, "SELECT * FROM testtable");
            assertThat(expectedValues.size(), is(0));
            assertThat(exception.getMessage(), is("Batch is empty."));
        }
    }

    @Test
    public void selects_with_no_bind_values() throws SQLException {

        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE TABLE testtable (first INTEGER NOT NULL)",
                "INSERT INTO testtable (first) VALUES (10)",
                "INSERT INTO testtable (first) VALUES (11)");

        List<Integer> result = dataStore.executeWithResult(connection ->
                connection.select("SELECT first FROM testtable ORDER BY first ASC",
                        rowStream -> rowStream.map(row -> asInteger(row.at("first"))).collect(toList()))
        );

        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(10));
        assertThat(result.get(1), is(11));
    }

    @Test
    public void selects_with_bind_values() throws SQLException {
        DataSourceInstantiationAndAccess.prepareData(dataSource, "CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)",
                "INSERT INTO testtable (first, second) VALUES (10, 'asecond10')",
                "INSERT INTO testtable (first, second) VALUES (11, 'asecond11')");

        List<Row> result = dataStore.executeWithResult(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first = ? AND second = ?",
                        rowStream -> rowStream.collect(toList()),
                        bind(10), bind("asecond10"))
        );

        assertThat(result.size(), is(1));
        assertThat(asInteger(result.get(0).at("first")), is(10));
        assertThat(asString(result.get(0).at("second")), is("asecond10"));
    }


}
