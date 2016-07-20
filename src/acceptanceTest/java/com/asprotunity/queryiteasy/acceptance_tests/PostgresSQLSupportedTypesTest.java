package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DefaultDataStore;
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

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bindLongVarbinary;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class PostgresSQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DefaultDataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Postgresql JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DefaultDataStore(dataSource);
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
        String binaryContent = "this is the content of the binary";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> inputStreamSupplier = () -> new ByteArrayInputStream(binaryContent.getBytes(charset));
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first BYTEA NULL, second BYTEA NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bindLongVarbinary(() -> null), bindLongVarbinary(inputStreamSupplier));
        });

        Function<InputStream, String> streamReader = inputStream -> StringIO.readFrom(inputStream, charset);

        List<Tuple2> expectedValues = getDataStore().executeWithResult(connection ->
                connection.select(row -> new Tuple2<>(row.fromBinaryStream(1, streamReader),
                                row.fromBinaryStream(2, streamReader)),
                        "SELECT * FROM testtable").collect(toList()));

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(binaryContent));
    }


    @Test
    public void stores_and_reads_text() throws SQLException, UnsupportedEncodingException {
        String text = "this is the content of the text";
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first TEXT NULL, second TEXT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind((String) null), bind(text));
        });

        List<Tuple2> expectedValues = getDataStore().executeWithResult(connection ->
                connection.select(row -> new Tuple2<>(row.asString("first"), row.asString("second")),
                        "SELECT * FROM testtable").collect(toList()));

        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(text));
    }

    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select(row -> row.asString("dropTableStatement"),
                    "select 'drop table if exists \"' || tablename || '\" cascade;' as dropTableStatement" +
                            "  from pg_tables " +
                            " where tableowner = 'testuser'"
            ).collect(toList());
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
