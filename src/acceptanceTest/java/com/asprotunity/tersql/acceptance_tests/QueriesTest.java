package com.asprotunity.tersql.acceptance_tests;


import com.asprotunity.tersql.connection.Batch;
import com.asprotunity.tersql.connection.Row;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static com.asprotunity.tersql.connection.Batch.batch;
import static com.asprotunity.tersql.connection.StatementParameter.bind;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public class QueriesTest extends EndToEndTestBase {


    @Test
    public void inserts_with_no_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (first) VALUES (10)");
        });

        List<Row> expectedValues = query("SELECT first FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
    }

    @Test
    public void inserts_null_integers_as_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind((Integer) null), bind("asecond"));
        });

        List<Row> expectedValues = query("SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asString("second"), is("asecond"));
    }

    @Test
    public void does_batch_inserts() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    batch(bind(10), bind("asecond10")),
                    batch(bind(11), bind("asecond11")),
                    batch(bind(12), bind("asecond12")));
        });

        List<Row> expectedValues = query("SELECT * FROM testtable ORDER BY first ASC");
        assertThat(expectedValues.size(), is(3));
        for (int first = 0; first < expectedValues.size(); ++first) {
            assertThat(expectedValues.get(first).asInteger("first"), is(first + 10));
            assertThat(expectedValues.get(first).asString("second"), is("asecond" + (first + 10)));
        }
    }

    @Test
    public void queries_with_null_integer_values() throws SQLException {

        prepareExpectedData("CREATE TABLE testtable (first INTEGER NULL, second VARCHAR(20) NOT NULL)",
                "INSERT INTO testtable (first, second) VALUES (null, 'asecond')");

        List<Row> result = dataStore.executeWithResult(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first is NULL",
                        rowStream -> rowStream.collect(toList())
                ));

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asInteger("first"), is(nullValue()));
        assertThat(result.get(0).asString("second"), is("asecond"));
    }

    @Test
    public void inserts_null_double_as_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind((Double) null), bind("asecond"));
        });

        List<Row> expectedValues = query("SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(nullValue()));
        assertThat(expectedValues.get(0).asString("second"), is("asecond"));
    }

    @Test
    public void inserts_long_as_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(1234456L), bind("asecond"));
        });

        List<Row> expectedValues = query("SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asLong("first"), is(1234456L));
        assertThat(expectedValues.get(0).asString("second"), is("asecond"));
    }

    @Test
    public void inserts_double_as_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(1.7d), bind("asecond"));
        });

        List<Row> expectedValues = query("SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asDouble("first"), is(closeTo(1.7d, 0.00000000001d)));
        assertThat(expectedValues.get(0).asString("second"), is("asecond"));
    }

    @Test
    public void queries_with_null_double_values() throws SQLException {

        prepareExpectedData("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)",
                "INSERT INTO testtable (first, second) VALUES (null, 'asecond')");

        List<Row> result = dataStore.executeWithResult(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first is NULL",
                        rowStream -> rowStream.collect(toList())
                ));

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asDouble("first"), is(nullValue()));
        assertThat(result.get(0).asString("second"), is("asecond"));
    }


}
