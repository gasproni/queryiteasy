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
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public abstract class SupportedTypesTestBase {

    @After
    public void tearDown() throws Exception {
        cleanup();
    }

    @Test
    public void handles_integers_correctly() throws SQLException {
        Integer value = 10;
        List<Row> expectedValues = storeAndReadValuesBack("INTEGER", bind((Integer) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asInteger("second"), is(value));
    }

    @Test
    public void handles_short_integers_correctly() throws SQLException {
        Short value = 10;
        List<Row> expectedValues = storeAndReadValuesBack("SMALLINT", bind((Short) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asShort("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asShort("second"), is(value));
    }

    @Test
    public void handles_doubles_mapped_to_float_correctly() throws SQLException {
        Double value = 10.0;
        List<Row> expectedValues = storeAndReadValuesBack("FLOAT", bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDouble("second"), is(value));
    }

    @Test
    public void handles_floats_correctly() throws SQLException {
        Float value = 10.0F;
        List<Row> expectedValues = storeAndReadValuesBack("REAL", bind((Float) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asFloat("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asFloat("second"), is(value));
    }

    @Test
    public void handles_big_decimals_as_decimal_correctly() throws SQLException {
        BigDecimal value = new BigDecimal(10);
        List<Row> expectedValues = storeAndReadValuesBack("DECIMAL", bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asBigDecimal("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asBigDecimal("second"), is(value));
    }

    @Test
    public void handles_big_decimals_as_numeric_correctly() throws SQLException {
        BigDecimal value = new BigDecimal(10);
        List<Row> expectedValues = storeAndReadValuesBack("NUMERIC", bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asBigDecimal("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asBigDecimal("second"), is(value));
    }

    @Test
    public void handles_dates_correctly() throws SQLException {
        // Fri, 01 Jan 2016 00:00:00 GMT It's important that the time is all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Date value = new Date(1451606400000L);
        List<Row> expectedValues = storeAndReadValuesBack("DATE", bind((Date) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDate("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asDate("second"), is(value));
    }

    @Test
    public void handles_blobs_correctly() throws SQLException, UnsupportedEncodingException {
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
            assertThat(expectedValues.get(0).fromBlob("first", optInputStream ->
                    SupportedTypesTestBase.readFrom(optInputStream, charset.name())), is(nullValue()));
            assertThat(expectedValues.get(0).fromBlob("second",
                    optInputStream -> SupportedTypesTestBase.readFrom(optInputStream, charset.name())), is(blobContent));
            assertThat(expectedValues.get(0).fromBlob("second",
                    optInputStream -> SupportedTypesTestBase.readFrom(optInputStream, charset.name())), is(blobContent));

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

    private static String readFrom(Optional<InputStream> inputStream, String charset) {
        return inputStream.map(stream -> new java.util.Scanner(stream, charset).useDelimiter("\\A").next()).orElse(null);
    }

    protected abstract void cleanup() throws Exception;

    protected abstract DataStore getDataStore();

}
