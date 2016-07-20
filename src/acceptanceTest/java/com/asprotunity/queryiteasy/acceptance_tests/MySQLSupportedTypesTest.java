package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DefaultDataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.stringio.StringIO;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.*;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asByte;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asDouble;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class MySQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DefaultDataStore dataStore;
    private static String dbName;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No MySQL JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DefaultDataStore(dataSource);
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("mysql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.mysql.datasource.class"));

        String dbURL = properties.getProperty("queryiteasy.mysql.datasource.url");
        Method setUrl = result.getClass().getMethod("setURL", String.class);
        setUrl.invoke(result, dbURL);

        dbName = dbURL.substring(dbURL.lastIndexOf("/") + 1);

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.mysql.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.mysql.datasource.password"));
        return result;

    }

    @Test
    public void stores_and_reads_doubles_mapped_to_double() throws SQLException {
        Double value = 10.0;
        List<Tuple2<Double, Double>> expectedValues = storeAndReadValuesBack("DOUBLE", Row::asDouble, bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDouble(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asDouble(expectedValues.get(0)._2), is(value));
    }

    @Test
    public void stores_and_reads_bytes_as_tinyints() throws SQLException {
        Byte value = 's';
        List<Tuple2<Byte, Byte>> expectedValues = storeAndReadValuesBack("TINYINT", Row::asByte, bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asByte(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asByte(expectedValues.get(0)._2), is(value));
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
    public void stores_and_reads_text_as_clobs() throws SQLException, UnsupportedEncodingException {
        String clobContent = "this is the content of the blob";
        Supplier<Reader> readerSupplier = () -> new StringReader(clobContent);

        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first TEXT NULL, second TEXT NULL)");
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
            List<String> dropTableStatements = connection.select(
                    row -> row.asString("dropTableStatement"),
                    "SELECT CONCAT('DROP TABLE ', table_name, ' CASCADE') as dropTableStatement" +
                            " FROM information_schema.tables WHERE table_schema = ?",
                    bind(dbName)).collect(toList());
            for (String statement : dropTableStatements) {
                connection.update(statement);
            }
        });
    }

    @Override
    protected DefaultDataStore getDataStore() {
        return dataStore;
    }

}
