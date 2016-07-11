package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
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
import java.util.stream.Collectors;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.*;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class MySQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DataStore dataStore;
    private static String dbName;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No MySQL JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
    }

    @Test
    public void stores_and_reads_doubles_mapped_to_double() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("DOUBLE", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDouble(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asDouble(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_bytes_as_tinyints() throws SQLException {
        Byte value = 's';
        List<Row> expectedValues = storeAndReadValuesBack("TINYINT", bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asByte(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asByte(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_blobs() throws SQLException, UnsupportedEncodingException {
        String blobContent = "this is the content of the blob";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> value = () -> new ByteArrayInputStream(blobContent.getBytes(charset));
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first BLOB NULL, second BLOB NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bindBlob(() -> null), bindBlob(value));
        });

        getDataStore().execute(connection -> {
            List<Row> expectedValues = connection.select(rowStream -> rowStream.collect(toList()), "SELECT * FROM testtable"
            );
            assertThat(expectedValues.size(), is(1));
            Function<InputStream, String> blobReader = inputStream -> StringIO.readFrom(inputStream, charset);
            assertThat(fromBlob(expectedValues.get(0).at("first"), blobReader), is(nullValue()));
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader), is(blobContent));
        });
    }

    @Test
    public void stores_and_reads_clobs_as_text() throws SQLException, UnsupportedEncodingException {
        String clobContent = "this is the content of the blob";
        Supplier<Reader> readerSupplier = () -> new StringReader(clobContent);

        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first TEXT NULL, second TEXT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bindClob(() -> null), bindClob(readerSupplier));
        });


        getDataStore().execute(connection -> {
            List<Row> expectedValues = connection.select(rowStream -> rowStream.collect(toList()),
                    "SELECT * FROM testtable");


            assertThat(expectedValues.size(), is(1));
            assertThat(fromClob(expectedValues.get(0).at("first"), StringIO::readFrom), is(nullValue()));
            assertThat(fromClob(expectedValues.get(0).at("second"), StringIO::readFrom), is(clobContent));
        });
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select(
                    rowStream -> rowStream.map(row -> asString(row.at("dropTableStatement"))).collect(Collectors.toList()),
                    "SELECT CONCAT('DROP TABLE ', table_name, ' CASCADE') as dropTableStatement" +
                            " FROM information_schema.tables WHERE table_schema = ?",
                    bind(dbName));
            for (String statement : dropTableStatements) {
                connection.update(statement);
            }
        });
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
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

}
