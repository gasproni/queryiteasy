package com.asprotunity.queryiteasy.acceptance_tests;


import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.ResultSetReaders;
import com.asprotunity.queryiteasy.io.StringIO;
import org.junit.After;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.asprotunity.queryiteasy.acceptance_tests.HSQLInMemoryConfigurationAndSchemaDrop.dropHSQLPublicSchema;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.*;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.fromBlob;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.fromClob;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SupportedTypesTestDelegate {

    private DataStore dataStore;

    public SupportedTypesTestDelegate(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @After
    public void tearDown() throws Exception {
        dropHSQLPublicSchema(dataStore);
    }


    public void stores_and_reads_doubles_mapped_to_double(String doubleSQLType) throws SQLException {
        Double value = 10.0;
        List<Tuple2<Double, Double>> foundValues =
                storeAndReadValuesBack(doubleSQLType, ResultSetReaders::asDouble, bind((Double) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_bytes_as(String sqlType) throws SQLException {
        Byte value = 's';
        List<Tuple2<Byte, Byte>> foundValues =
                storeAndReadValuesBack(sqlType, ResultSetReaders::asByte, bind((Byte) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }

    public void stores_and_reads_bytes_arrays(String sqlType) throws SQLException {
        byte[] value = new byte[]{1, 2, 3, 4};
        List<Tuple2<byte[], byte[]>> foundValues =
                storeAndReadValuesBack(sqlType, ResultSetReaders::asByteArray, bind((byte[]) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_blobs_as(String blobSQLType) throws SQLException, UnsupportedEncodingException {
        String blobContent = "this is the content of the blob";
        Charset charset = Charset.forName("UTF-8");
        Supplier<InputStream> inputStreamSupplier = () -> new ByteArrayInputStream(blobContent.getBytes(charset));

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first " + blobSQLType + " NULL, second " + blobSQLType + " NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                              bindBlob(() -> null), bindBlob(inputStreamSupplier));
        });

        Function<InputStream, String> blobReader = inputStream -> StringIO.readFrom(inputStream, charset);

        List<Tuple2> foundValues =
                dataStore.executeWithResult(
                        connection -> connection.select(rs -> new Tuple2<>(fromBlob(rs, 1, blobReader),
                                                                           fromBlob(rs, 2, blobReader)),
                                                        "SELECT * FROM testtable").collect(toList())
                );

        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(blobContent));
    }


    public void stores_and_reads_clobs(String clobSQLType) throws SQLException, UnsupportedEncodingException {
        String clobContent = "this is the content of the blob";
        Supplier<Reader> readerSupplier = () -> new StringReader(clobContent);

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first " + clobSQLType + " NULL, second " + clobSQLType + " NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                              bindClob(() -> null), bindClob(readerSupplier));
        });


        List<Tuple2> foundValues =
                dataStore.executeWithResult(
                        connection -> connection.select(rs -> new Tuple2<>(fromClob(rs,
                                                                                    1,
                                                                                    StringIO::readFrom),
                                                                           fromClob(rs,
                                                                                    2,
                                                                                    StringIO::readFrom)),
                                                        "SELECT * FROM testtable").collect(toList())
                );

        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(clobContent));
    }


    public void stores_and_reads_longs_as_bigints() throws SQLException {
        Long value = 10L;
        List<Tuple2<Long, Long>> foundValues =
                storeAndReadValuesBack("BIGINT", ResultSetReaders::asLong, bind((Long) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_booleans() throws SQLException {
        List<Tuple2<Boolean, Boolean>> foundValues =
                storeAndReadValuesBack("BOOLEAN", ResultSetReaders::asBoolean, bind((Boolean) null), bind(true));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertTrue(foundValues.get(0)._2);
    }


    public void stores_and_reads_times(String timeSQLType) throws SQLException {
        // 01 Jan 1970 10:11:12.000 GMT. It's important that the milliseconds are all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Time value = new Time(36672000L);
        List<Tuple2<Time, Time>> foundValues =
                storeAndReadValuesBack(timeSQLType, ResultSetReaders::asTime, bind((Time) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_timestamps() throws SQLException {
        // Tue, 12 Jan 2016 10:11:12.000 GMT. Note that some DBs support the milliseconds.
        Timestamp value = new Timestamp(1452593472000L);
        List<Tuple2<Timestamp, Timestamp>> foundValues =
                storeAndReadValuesBack("TIMESTAMP", ResultSetReaders::asTimestamp, bind((Timestamp) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_integers() throws SQLException {
        Integer value = 10;
        List<Tuple2<Integer, Integer>> foundValues =
                storeAndReadValuesBack("INTEGER", ResultSetReaders::asInteger, bind((Integer) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_strings() throws SQLException {
        String value = "this is the text";
        List<Tuple2<String, String>> foundValues =
                storeAndReadValuesBack("VARCHAR(250)", ResultSetReaders::asString, bind((String) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_short_integers() throws SQLException {
        Short value = 10;
        List<Tuple2<Short, Short>> foundValues =
                storeAndReadValuesBack("SMALLINT", ResultSetReaders::asShort, bind((Short) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_doubles_as_floats() throws SQLException {
        Double value = 10.0;
        List<Tuple2<Double, Double>> foundValues =
                storeAndReadValuesBack("FLOAT", ResultSetReaders::asDouble, bind((Double) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_floats() throws SQLException {
        Float value = 10.0F;
        List<Tuple2<Float, Float>> foundValues =
                storeAndReadValuesBack("REAL", ResultSetReaders::asFloat, bind((Float) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_big_decimals_as_decimal() throws SQLException {
        BigDecimal value = BigDecimal.TEN;
        List<Tuple2<BigDecimal, BigDecimal>> foundValues =
                storeAndReadValuesBack("DECIMAL", ResultSetReaders::asBigDecimal, bind((BigDecimal) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_big_decimals_as_numeric() throws SQLException {
        BigDecimal value = BigDecimal.TEN;
        List<Tuple2<BigDecimal, BigDecimal>> foundValues =
                storeAndReadValuesBack("NUMERIC", ResultSetReaders::asBigDecimal, bind((BigDecimal) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_dates() throws SQLException {
        // Fri, 01 Jan 2016 00:00:00 GMT It's important that the time is all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Date value = new Date(1451606400000L);
        List<Tuple2<Date, Date>> foundValues =
                storeAndReadValuesBack("DATE", ResultSetReaders::asDate, bind((Date) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }


    public void stores_and_reads_bytes_as_smallints() throws SQLException {
        Byte value = 's';
        List<Tuple2<Byte, Byte>> foundValues =
                storeAndReadValuesBack("SMALLINT", ResultSetReaders::asByte, bind((Byte) null), bind(value));
        assertThat(foundValues.size(), is(1));
        assertThat(foundValues.get(0)._1, is(nullValue()));
        assertThat(foundValues.get(0)._2, is(value));
    }

    private <Type1> List<Tuple2<Type1, Type1>> storeAndReadValuesBack(String sqlType, BiFunction<ResultSet, Integer, Type1> rowMapper,
                                                                      InputParameter firstValue, InputParameter secondValue) {
        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first " + sqlType + " NULL, second " + sqlType + " NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                              firstValue, secondValue);
        });

        return dataStore.executeWithResult(connection ->
                                                   connection.select(rs -> new Tuple2<>(rowMapper.apply(rs, 1), rowMapper.apply(rs, 2)),
                                                                     "SELECT * FROM testtable").collect(toList())
        );
    }
}
