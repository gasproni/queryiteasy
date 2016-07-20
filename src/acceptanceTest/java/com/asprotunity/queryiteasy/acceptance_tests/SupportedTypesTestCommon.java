package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DefaultDataStore;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.Row;
import org.junit.After;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
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
        List<Tuple2<Integer, Integer>> expectedValues = storeAndReadValuesBack("INTEGER", Row::asInteger, bind((Integer) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asInteger(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asInteger(expectedValues.get(0)._2), is(value));
    }

    @Test
    public void stores_and_reads_strings() throws SQLException {
        String value = "this is the text";
        List<Tuple2<String, String>> expectedValues = storeAndReadValuesBack("VARCHAR(250)", Row::asString, bind((String) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_short_integers() throws SQLException {
        Short value = 10;
        List<Tuple2<Short, Short>> expectedValues = storeAndReadValuesBack("SMALLINT", Row::asShort, bind((Short) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asShort(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asShort(expectedValues.get(0)._2), is(value));
    }

    @Test
    public void stores_and_reads_doubles_as_floats() throws SQLException {
        Double value = 10.0;
        List<Tuple2<Double, Double>> expectedValues = storeAndReadValuesBack("FLOAT", Row::asDouble, bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDouble(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asDouble(expectedValues.get(0)._2), is(value));
    }

    @Test
    public void stores_and_reads_floats() throws SQLException {
        Float value = 10.0F;
        List<Tuple2<Float, Float>> expectedValues = storeAndReadValuesBack("REAL", Row::asFloat, bind((Float) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asFloat(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asFloat(expectedValues.get(0)._2), is(value));
    }

    @Test
    public void stores_and_reads_big_decimals_as_decimal() throws SQLException {
        BigDecimal value = BigDecimal.TEN;
        List<Tuple2<BigDecimal, BigDecimal>> expectedValues = storeAndReadValuesBack("DECIMAL", Row::asBigDecimal, bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_big_decimals_as_numeric() throws SQLException {
        BigDecimal value = BigDecimal.TEN;
        List<Tuple2<BigDecimal, BigDecimal>> expectedValues = storeAndReadValuesBack("NUMERIC", Row::asBigDecimal, bind((BigDecimal) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_dates() throws SQLException {
        // Fri, 01 Jan 2016 00:00:00 GMT It's important that the time is all zeroes
        // or that will be lost when putting the value in the db
        // and the assert will fail.
        Date value = new Date(1451606400000L);
        List<Tuple2<Date, Date>> expectedValues = storeAndReadValuesBack("DATE", Row::asDate, bind((Date) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0)._1, is(nullValue()));
        assertThat(expectedValues.get(0)._2, is(value));
    }

    @Test
    public void stores_and_reads_doubles_as_double_precision() throws SQLException {
        Double value = 10.0;
        List<Tuple2<Double, Double>> expectedValues = storeAndReadValuesBack("DOUBLE PRECISION", Row::asDouble, bind((Double) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asDouble(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asDouble(expectedValues.get(0)._2), is(value));
    }

    @Test
    public void stores_and_reads_bytes_as_smallints() throws SQLException {
        Byte value = 's';
        List<Tuple2<Byte, Byte>> expectedValues = storeAndReadValuesBack("SMALLINT", Row::asByte, bind((Byte) null), bind(value));
        assertThat(expectedValues.size(), is(1));
        assertThat(asByte(expectedValues.get(0)._1), is(nullValue()));
        assertThat(asByte(expectedValues.get(0)._2), is(value));
    }


    protected <Type1> List<Tuple2<Type1, Type1>> storeAndReadValuesBack(String sqlType, BiFunction<Row, Integer, Type1> rowMapper,
                                                                        InputParameter firstValue, InputParameter secondValue) {
        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first " + sqlType + " NULL, second " + sqlType + " NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    firstValue, secondValue);
        });

        return getDataStore().executeWithResult(connection ->
                connection.select(row -> new Tuple2<>(rowMapper.apply(row, 1), rowMapper.apply(row, 2)),
                        "SELECT * FROM testtable").collect(toList())
        );
    }

    protected abstract void cleanup() throws Exception;

    protected abstract DefaultDataStore getDataStore();

}
