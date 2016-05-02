package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
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
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.loadProperties;
import static com.asprotunity.queryiteasy.acceptance_tests.TestPropertiesLoader.prependTestDatasourcesConfigFolderPath;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.asString;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.fromBlob;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class PostgresSQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DataStore dataStore;
    private static String dbName;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureDataSource();
        assumeTrue("No Postgresql JDBC driver found, skipping tests", dataSource != null);
        dataStore = new DataStore(dataSource);
    }

    @Test
    public void stores_and_reads_blobs_as_bytea() throws SQLException, UnsupportedEncodingException {
        String blobContent = "this is the content of the blob";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> value = () -> new ByteArrayInputStream(blobContent.getBytes(charset));
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first bytea NULL, second bytea NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(() -> null), bind(value));
        });

        getDataStore().execute(connection -> {
            List<Row> expectedValues = connection.select("SELECT * FROM testtable",
                    rowStream -> rowStream.collect(toList()));
            assertThat(expectedValues.size(), is(1));
            Function<Optional<InputStream>, String> blobReader2 = optInputStream ->
                    readFrom(optInputStream, charset.name());
            assertThat(fromBlob(expectedValues.get(0).at("first"), blobReader2), is(nullValue()));
            Function<Optional<InputStream>, String> blobReader1 = optInputStream -> readFrom(optInputStream, charset.name());
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader1), is(blobContent));
            Function<Optional<InputStream>, String> blobReader = optInputStream -> readFrom(optInputStream, charset.name());
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader), is(blobContent));

        });
    }
    @Override
    protected void cleanup() throws Exception {
        getDataStore().execute(connection -> {
            List<String> dropTableStatements = connection.select("select 'drop table if exists \"' || tablename || '\" cascade;' as dropTableStatement" +
                            "  from pg_tables " +
                            " where tableowner = 'testuser'",
                    rowStream -> rowStream.map(row -> asString(row.at("dropTableStatement"))).collect(Collectors.toList()));
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

        Path path = prependTestDatasourcesConfigFolderPath("postgresql.properties");
        if (!Files.exists(path)) {
            return null;
        }
        Properties properties = loadProperties(path);

        DataSource result = DataSourceInstantiationAndAccess.instantiateDataSource(properties.getProperty("queryiteasy.postgresql.datasource.class"));

        dbName = properties.getProperty("queryiteasy.postgresql.databaseName");
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

}
