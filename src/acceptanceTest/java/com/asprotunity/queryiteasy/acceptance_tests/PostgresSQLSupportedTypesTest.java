package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.stringio.StringIO;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
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
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bindLongVarbinary;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asString;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromLongVarbinary;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class PostgresSQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Postgresql JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
    }

    private static DataSource configureDataSource() throws Exception {

        Path path = prependTestDatasourcesConfigFolderPath("postgresql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.postgresql.datasource.class"));

        String dbName = properties.getProperty("queryiteasy.postgresql.databaseName");
        Method setDbName = result.getClass().getMethod("setDatabaseName", String.class);
        setDbName.invoke(result, dbName);

        Method setServerName = result.getClass().getMethod("setServerName", String.class);
        setServerName.invoke(result, properties.getProperty("queryiteasy.postgresql.serverName"));

        Method setUser = result.getClass().getMethod("setUser", String.class);
        setUser.invoke(result, properties.getProperty("queryiteasy.postgresql.datasource.user"));

        Method setPassword = result.getClass().getMethod("setPassword", String.class);
        setPassword.invoke(result, properties.getProperty("queryiteasy.postgresql.datasource.password"));
        return result;

    }

    @Test
    public void stores_and_reads_longvarbinaries() throws SQLException, UnsupportedEncodingException {
        String byteaContent = "this is the content of the bytea";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> inputStreamSupplier = () -> new ByteArrayInputStream(byteaContent.getBytes(charset));
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first BYTEA NULL, second BYTEA NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bindLongVarbinary(() -> null), bindLongVarbinary(inputStreamSupplier));
        });

        getDataStore().execute(connection -> {
            List<Row> expectedValues = connection.select(rowStream -> rowStream.collect(toList()), "SELECT * FROM testtable"
            );
            assertThat(expectedValues.size(), is(1));
            Function<InputStream, String> blobReader = inputStream -> StringIO.readFrom(inputStream, charset);
            assertThat(fromLongVarbinary(expectedValues.get(0).at("first"), blobReader), is(nullValue()));
            assertThat(fromLongVarbinary(expectedValues.get(0).at("second"), blobReader), is(byteaContent));

        });
    }

    @Test
    public void stores_and_reads_text() throws SQLException, UnsupportedEncodingException {
        String text = "this is the content of the text";
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first TEXT NULL, second TEXT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind((String) null), bind(text));
        });

        getDataStore().execute(connection -> {
            List<Row> expectedValues = connection.select(rowStream -> rowStream.collect(toList()), "SELECT * FROM testtable"
            );
            assertThat(expectedValues.size(), is(1));
            assertThat(asString(expectedValues.get(0).at("second")), is(text));
            assertThat(asString(expectedValues.get(0).at("first")), is(nullValue()));
        });
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select(rowStream ->
                            rowStream.map(row -> asString(row.at("dropTableStatement"))).collect(Collectors.toList()),
                    "select 'drop table if exists \"' || tablename || '\" cascade;' as dropTableStatement" +
                            "  from pg_tables " +
                            " where tableowner = 'testuser'"
            );
            for (String statement : dropTableStatements) {
                connection.update(statement);
            }
        });
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
    }

}
