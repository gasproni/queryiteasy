package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.connection.RuntimeSQLException;
import org.junit.After;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterDefaultBinders.bind;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class QueriesTestBase {

    @After
    public void tearDown() throws Exception {
        cleanup();
    }

    protected abstract void cleanup() throws Exception;

    protected abstract DataSource getDataSource();

    protected abstract DataStore getDataStore();


    @Test
    public void inserts_with_no_bind_values() throws SQLException {

        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (first) VALUES (10)");
        });

        List<Row> expectedValues = DataSourceHelpers.query(getDataSource(), "SELECT first FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
    }

    @Test
    public void rolls_back_transaction_when_exception_thrown() throws SQLException {

        try {
            getDataStore().execute(connection -> {
                connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
                connection.update("INSERT INTO testtable (first) VALUES (10)");
                throw new RuntimeException();
            });
            fail("Exception expected");
        } catch (RuntimeException ex) {
            List<Row> expectedValues = DataSourceHelpers.query(getDataSource(), "SELECT first FROM testtable");
            assertThat(expectedValues.size(), is(0));
        }
    }

    @Test
    public void inserts_with_some_bind_values() throws SQLException {

        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(10), bind("asecond"));
        });

        List<Row> expectedValues = DataSourceHelpers.query(getDataSource(), "SELECT * FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
        assertThat(expectedValues.get(0).asString("second"), is("asecond"));
    }

    @Test
    public void does_batch_inserts() throws SQLException {

        getDataStore().execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    Arrays.asList(batch(bind(10), bind("asecond10")),
                            batch(bind(11), bind("asecond11")),
                            batch(bind(12), bind("asecond12"))));
        });

        List<Row> expectedValues = DataSourceHelpers.query(getDataSource(), "SELECT * FROM testtable ORDER BY first ASC");
        assertThat(expectedValues.size(), is(3));
        for (int index = 0; index < expectedValues.size(); ++index) {
            assertThat(expectedValues.get(index).asInteger("first"), is(index + 10));
            assertThat(expectedValues.get(index).asString("second"), is("asecond" + (index + 10)));
        }
    }

    @Test
    public void throws_exception_when_batch_is_empty() throws SQLException {

        try {
            getDataStore().execute(connection -> {
                connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
                connection.update("INSERT INTO testtable (first, second) VALUES (1, 'sometext')",
                        Collections.emptyList());
            });
            fail("RuntimeSQLException expected!");
        } catch (RuntimeSQLException exception) {
            List<Row> expectedValues = DataSourceHelpers.query(getDataSource(), "SELECT * FROM testtable");
            assertThat(expectedValues.size(), is(0));
            assertThat(exception.getMessage(), is("Batch is empty."));
        }
    }

    @Test
    public void selects_with_no_bind_values() throws SQLException {

        DataSourceHelpers.prepareData(getDataSource(), "CREATE TABLE testtable (first INTEGER NOT NULL)",
                "INSERT INTO testtable (first) VALUES (10)",
                "INSERT INTO testtable (first) VALUES (11)");

        List<Integer> result = getDataStore().executeWithResult(connection ->
                connection.select("SELECT first FROM testtable ORDER BY first ASC",
                        rowStream -> rowStream.map(row -> row.asInteger("first")).collect(toList()))
        );

        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(10));
        assertThat(result.get(1), is(11));
    }

    @Test
    public void selects_with_bind_values() throws SQLException {
        DataSourceHelpers.prepareData(getDataSource(), "CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)",
                "INSERT INTO testtable (first, second) VALUES (10, 'asecond10')",
                "INSERT INTO testtable (first, second) VALUES (11, 'asecond11')");

        List<Row> result = getDataStore().executeWithResult(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first = ? AND second = ?",
                        rowStream -> rowStream.collect(toList()),
                        bind(10), bind("asecond10"))
        );

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asInteger("first"), is(10));
        assertThat(result.get(0).asString("second"), is("asecond10"));
    }


}
