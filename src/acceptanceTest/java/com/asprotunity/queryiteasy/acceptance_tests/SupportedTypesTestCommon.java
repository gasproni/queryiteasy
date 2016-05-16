package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.Row;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static com.asprotunity.queryiteasy.connection.SQLDataConverters.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public abstract class SupportedTypesTestCommon {

    @After
    public void tearDown() throws Exception {
        cleanup();
    }

    @Test
    public void stores_and_reads_integers() throws SQLException {
        Integer value = 10;
        List<Row> expectedValues = storeAndReadValuesBack("INTEGER", bind((Integer) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asInteger(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asInteger(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_short_integers() throws SQLException {
        Short value = 10;
        List<Row> expectedValues = storeAndReadValuesBack("SMALLINT", bind((Short) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asShort(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asShort(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_doubles_as_floats() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("FLOAT", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDouble(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asDouble(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_floats() throws SQLException {
        Float value = 10.0F;
        List<Row> expectedValues = storeAndReadValuesBack("REAL", bind((Float) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asFloat(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asFloat(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_big_decimals_as_decimal() throws SQLException {
        BigDecimal value = BigDecimal.TEN;
        List<Row> expectedValues = storeAndReadValuesBack("DECIMAL", bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asBigDecimal(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asBigDecimal(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_big_decimals_as_numeric() throws SQLException {
        BigDecimal value = BigDecimal.TEN;
        List<Row> expectedValues = storeAndReadValuesBack("NUMERIC", bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asBigDecimal(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asBigDecimal(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_dates() throws SQLException {
        // Fri, 01 Jan 2016 00:00:00 GMT It's important that the time is all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Date value = new Date(1451606400000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Date) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDate(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asDate(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_doubles_as_double_precision() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("DOUBLE PRECISION", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDouble(expectedValues.get(0).at("first")), is(nullValue()));
        assertThat(asDouble(expectedValues.get(0).at("second")), is(value));
    }

    @Test
    public void stores_and_reads_bytes_as_smallints() throws SQLException {
        Byte value = 's';
        List<Row> expectedValues = storeAndReadValuesBack("SMALLINT", bind((Byte) null), bind(value));
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
            List<Row> expectedValues = connection.select("SELECT * FROM testtable",
                    rowStream -> rowStream.collect(toList()));
            assertThat(expectedValues.size(), is(1));
            Function<Optional<InputStream>, String> blobReader = optInputStream -> readFrom(optInputStream, charset.name());
            assertThat(fromBlob(expectedValues.get(0).at("first"), blobReader), is(nullValue()));
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader), is(blobContent));
            assertThat(fromBlob(expectedValues.get(0).at("second"), blobReader), is(blobContent));

        });
    }

    protected List<Row> storeAndReadValuesBack(String sqlType, InputParameter firstValue, InputParameter secondValue) {
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first " + sqlType + " NULL, second " + sqlType + " NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    firstValue, secondValue);
        });

        return getDataStore().executeWithResult(connection ->
                connection.select("SELECT * FROM testtable", rowStream -> rowStream.collect(toList()))
        );
    }

    protected static String readFrom(Optional<InputStream> inputStream, String charset) {
        return inputStream.map(stream -> new java.util.Scanner(stream, charset).useDelimiter("\\A").next()).orElse(null);
    }

    protected abstract void cleanup() throws Exception;

    protected abstract DataStore getDataStore();

}
