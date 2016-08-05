package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.connection.BlobInputOutputParameter;
import com.asprotunity.queryiteasy.connection.BlobOutputParameter;
import com.asprotunity.queryiteasy.connection.LongVarBinaryInputOutputParameter;
import com.asprotunity.queryiteasy.connection.LongVarBinaryOutputParameter;
import com.asprotunity.queryiteasy.datastore.DataStore;
import com.asprotunity.queryiteasy.io.StringIO;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;
import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.*;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.*;
import static java.util.Arrays.asList;
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
        final DataSource dataSource1 = dataSource;
        dataStore = new DataStore(dataSource1);
    }

    @After
    public void tearDown() throws Exception {
        dropHSQLPublicSchema(dataStore);
    }

    @Test
    public void updates_with_no_bind_values() {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (intvalue INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (intvalue) VALUES (10)");
        });

        List<Integer> found =
                dataStore.executeWithResult(connection -> connection.select(rs -> asInteger(rs, 1),
                                                                            "SELECT * FROM testtable").collect(toList()));
        assertThat(found.size(), is(1));
        assertThat(found.get(0), is(10));
    }

    @Test
    public void updates_with_bind_values() {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (intvalue INTEGER NOT NULL, textvalue VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (intvalue, textvalue) VALUES (?, ?)",
                              bindInteger(10), bindString("text"));
        });

        List<Tuple2<Integer, String>> found =
                dataStore.executeWithResult(connection -> connection.select(rs -> new Tuple2<>(asInteger(rs, 1), asString(rs, 2)),
                                                                            "SELECT * FROM testtable").collect(toList()));
        assertThat(found.size(), is(1));
        assertThat(found.get(0)._1, is(10));
        assertThat(found.get(0)._2, is("text"));
    }

    @Test
    public void rolls_back_transaction_when_exception_thrown() {

        try {
            dataStore.execute(connection -> {
                connection.update("CREATE TABLE testtable (intvalue INTEGER NOT NULL)");
                connection.update("INSERT INTO testtable (intvalue) VALUES (10)");
                throw new RuntimeException();
            });
            fail("Exception expected");
        } catch (RuntimeException ex) {
            List<Integer> found = dataStore.executeWithResult(
                    connection -> connection.select(rs -> asInteger(rs, "intvalue"),
                                                    "SELECT intvalue FROM testtable").collect(toList())
            );
            assertThat(found.size(), is(0));
        }
    }

    @Test
    public void does_batch_updates() {
        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (intvalue INTEGER NOT NULL, textvalue VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (intvalue, textvalue) VALUES (?, ?)",
                              asList(batch(bindInteger(10), bindString("text10")),
                                     batch(bindInteger(11), bindString("text11")),
                                     batch(bindInteger(12), bindString("text12"))));
        });

        List<Tuple2<Integer, String>> found =
                dataStore.executeWithResult(
                        connection -> connection.select(rs -> new Tuple2<>(asInteger(rs, 1), asString(rs, 2)),
                                                        "SELECT * FROM testtable ORDER BY intvalue ASC").collect(toList())
                );

        assertThat(found.size(), is(3));
        for (int index = 0; index < found.size(); ++index) {
            assertThat(found.get(index)._1, is(index + 10));
            assertThat(found.get(index)._2, is("text" + (index + 10)));
        }
    }

    @Test
    public void selects_with_no_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (intvalue INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (intvalue) VALUES (?)",
                              asList(batch(bindInteger(10)),
                                     batch(bindInteger(11))));
        });

        List<Integer> result =
                dataStore.executeWithResult(
                        connection -> connection.select(rs -> asInteger(rs, "intvalue"),
                                                        "SELECT intvalue FROM testtable ORDER BY intvalue ASC").collect(toList())
                );

        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(10));
        assertThat(result.get(1), is(11));
    }


    @Test
    public void selects_with_bind_values() throws SQLException {
        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (intvalue INTEGER NOT NULL, textvalue VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (intvalue, textvalue) VALUES (?, ?)",
                              asList(batch(bindInteger(10), bindString("text10")),
                                     batch(bindInteger(11), bindString("text11"))));
        });

        List<Tuple2<Integer, String>> result =
                dataStore.executeWithResult(
                        connection -> connection.select(rs -> new Tuple2<>(asInteger(rs, 1), asString(rs, 2)),
                                                        "SELECT intvalue, textvalue FROM testtable WHERE intvalue = ? AND textvalue = ?",
                                                        bindInteger(10), bindString("text10")).collect(toList())
                );

        assertThat(result.size(), is(1));
        assertThat(result.get(0)._1, is(10));
        assertThat(result.get(0)._2, is("text10"));
    }

    @Test
    public void calls_stored_procedure_with_blob_in_and_out_parameters() {
        dataStore.execute(
                connection -> connection.update("CREATE PROCEDURE test_blob_params(in inparam BLOB, " +
                                                        "                          out outparam BLOB, " +
                                                        "                          inout ioparam BLOB)\n" +
                                                        "MODIFIES SQL DATA\n" +
                                                        "BEGIN ATOMIC \n" +
                                                        "   SET outparam = ioparam;\n" +
                                                        "   SET ioparam = inparam;\n" +
                                                        " END")
        );

        String inParamContent = "this is the content of the inparam blob";
        Charset charset = Charset.forName("UTF-8");
        BlobOutputParameter<String> outParam = new BlobOutputParameter<>(inputStream -> StringIO.readFrom(inputStream,
                                                                                                          charset));
        String ioParamInitialContent = "this is the initial content of the ioParam blob";
        BlobInputOutputParameter<String> ioParam =
                new BlobInputOutputParameter<>(() -> new ByteArrayInputStream(ioParamInitialContent.getBytes(charset)),
                                               inputStream -> StringIO.readFrom(inputStream, charset));

        dataStore.execute(
                connection -> connection.call("{call test_blob_params(?, ?, ?)}",
                                              bindBlob(() -> new ByteArrayInputStream(inParamContent.getBytes(charset))),
                                              outParam,
                                              ioParam)
        );

        assertThat(outParam.value(), is(ioParamInitialContent));
        assertThat(ioParam.value(), is(inParamContent));
    }

    @Test
    public void calls_stored_procedure_with_longvarbinary_in_and_out_parameters() {
        dataStore.execute(
                connection -> connection.update("CREATE PROCEDURE test_longvarbinary_params(in inparam LONGVARBINARY, " +
                                                        "                                   out outparam LONGVARBINARY, " +
                                                        "                                   inout ioparam LONGVARBINARY)\n" +
                                                        "MODIFIES SQL DATA\n" +
                                                        "BEGIN ATOMIC \n" +
                                                        "   SET outparam = ioparam;\n" +
                                                        "   SET ioparam = inparam;\n" +
                                                        " END")
        );

        byte[] inParamContent = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[] ioParamInitialContent = {10, 11, 12, 13, 14, 15};
        LongVarBinaryInputOutputParameter ioParam =
                new LongVarBinaryInputOutputParameter(() -> new ByteArrayInputStream(ioParamInitialContent));

        LongVarBinaryOutputParameter outParam = new LongVarBinaryOutputParameter();

        dataStore.execute(connection ->
                                  connection.call("{call test_longvarbinary_params(?, ?, ?)}",
                                                  bindLongVarbinary(() -> new ByteArrayInputStream(inParamContent)),
                                                  outParam,
                                                  ioParam)
        );

        assertThat(outParam.value(), is(ioParamInitialContent));
        assertThat(ioParam.value(), is(inParamContent));
    }


    @Test
    public void calls_function_with_no_input_parameters_and_returns_result() throws ParseException {

        dataStore.execute(
                connection -> connection.update("CREATE FUNCTION return_date()\n" +
                                                        "RETURNS DATE\n" +
                                                        "BEGIN ATOMIC \n" +
                                                        "   RETURN TO_DATE('2016-06-23', 'YYYY-MM-DD');\n" +
                                                        " END")
        );

        Date found = dataStore.executeWithResult(
                connection -> connection.call(rs -> asDate(rs, 1),
                                              "{call return_date()}").findFirst().orElse(null)
        );

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date expected = new Date(df.parse("2016-06-23").getTime());
        assertThat(found, is(expected));
    }

    @Test
    public void calls_function_with_input_parameters_and_returns_result() throws ParseException {

        dataStore.execute(
                connection -> connection.update("CREATE FUNCTION return_inparam(in inparam VARCHAR(20))\n" +
                                                        "RETURNS VARCHAR(20)\n" +
                                                        "BEGIN ATOMIC \n" +
                                                        "   RETURN inparam;\n" +
                                                        " END")
        );

        String expected = "expectedResult";
        String result = dataStore.executeWithResult(
                connection -> connection.call(rs -> asString(rs, 1),
                                              "{call return_inparam(?)}", bindString(expected)).findFirst().orElse(null)
        );

        assertThat(result, is(expected));
    }


}
