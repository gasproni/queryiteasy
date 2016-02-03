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
import static org.junit.Assert.assertThat;

public class SupportedTypesTest extends EndToEndTestBase {


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
    public void inserts_with_some_bind_values() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(10), bind("asecond"));
        });

        List<Row> expectedValues = query("SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
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
    public void does_batch_inserts_with_batch_array() throws SQLException {

        dataStore.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");

            Batch firstBatch = batch(bind(10), bind("asecond10"));
            Batch[] batches = new Batch[2];
            for (int first = 0; first < 2; ++first) {
                int value = first + 11;
                batches[first] = batch(bind(value), bind("asecond" + value));
            }

            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)", firstBatch, batches);
        });

        List<Row> expectedValues = query("SELECT * FROM testtable ORDER BY first ASC");
        assertThat(expectedValues.size(), is(3));
        for (int first = 10; first < expectedValues.size(); ++first) {
            assertThat(expectedValues.get(first).asInteger("first"), is(first));
            assertThat(expectedValues.get(first).asString("second"), is("asecond" + first));
        }
    }


    @Test
    public void selects_with_no_bind_values() throws SQLException {

        prepareExpectedData("CREATE TABLE testtable (first INTEGER NOT NULL)",
                "INSERT INTO testtable (first) VALUES (10)",
                "INSERT INTO testtable (first) VALUES (11)");

        List<Integer> result = dataStore.executeWithResult(connection ->
                connection.select("SELECT first FROM testtable ORDER BY first ASC",
                        rowStream -> rowStream.map(row -> row.asInteger("first")).collect(toList()))
        );

        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(10));
        assertThat(result.get(1), is(11));
    }

    @Test
    public void selects_with_bind_values() throws SQLException {

        prepareExpectedData("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)",
                "INSERT INTO testtable (first, second) VALUES (10, 'asecond10')",
                "INSERT INTO testtable (first, second) VALUES (11, 'asecond11')");

        List<Row> result = dataStore.executeWithResult(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first = ? AND second = ?",
                        rowStream -> rowStream.collect(toList()),
                        bind(10), bind("asecond10"))
        );

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asInteger("first"), is(10));
        assertThat(result.get(0).asString("second"), is("asecond10"));
    }



}
