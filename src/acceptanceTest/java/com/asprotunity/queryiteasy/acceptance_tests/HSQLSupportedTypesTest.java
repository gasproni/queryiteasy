package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.configureHSQLInMemoryDataSource;
import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HSQLSupportedTypesTest extends NonStandardSupportedTypesTestCommon {

    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        DataSource dataSource = configureHSQLInMemoryDataSource();
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
                    bind(() -> null), bind(value));
        });

        getDataStore().execute(connection -> {
            List<Row> expectedValues = connection.select(rowStream -> rowStream.collect(toList()), "SELECT * FROM testtable"
            );
            assertThat(expectedValues.size(), is(1));
            Function<InputStream, String> blobReader = inputStream -> readFrom(inputStream, charset.name());
            assertThat(fromBlob(expectedValues.get(0).at("first"), blobReader), is(nullValue()));
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader), is(blobContent));
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader), is(blobContent));

        });
    }

    @Override
    protected void cleanup() throws Exception {
        dropHSQLPublicSchema(getDataStore());
    }

    @Override
    protected DataStore getDataStore() {
        return dataStore;
    }

}
