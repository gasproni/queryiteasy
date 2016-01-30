package com.asprotunity.jdbcunboiled.acceptance_tests;


import com.asprotunity.jdbcunboiled.TransactionExecutor;
import com.asprotunity.jdbcunboiled.connection.Batch;
import com.asprotunity.jdbcunboiled.connection.Row;
import com.asprotunity.jdbcunboiled.internal.WrappedResultSet;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.asprotunity.jdbcunboiled.connection.Batch.batch;
import static com.asprotunity.jdbcunboiled.connection.StatementParameter.bind;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public class TransactionExecutorEndToEndTest {

    private static JDBCDataSource dataSource;
    private static TransactionExecutor executor;

    @BeforeClass
    public static void setUp() {
        dataSource = new JDBCDataSource();
        dataSource.setDatabase("jdbc:hsqldb:mem:testdb");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        executor = new TransactionExecutor(dataSource);
    }

    @After
    public void tearDown() throws SQLException {
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE testtable");
        statement.close();
        connection.commit();
        connection.close();
    }


    @Test
    public void inserts_with_no_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (first) VALUES (10)");
        });

        List<Row> expectedValues = query("SELECT first FROM testtable");
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).asInteger("first"), is(10));
    }


    @Test
    public void inserts_with_some_bind_values() throws SQLException {

        executor.execute(connection -> {
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
    public void inserts_null_integers_as_bind_values() throws SQLException {

        executor.execute(connection -> {
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

        executor.execute(connection -> {
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

        executor.execute(connection -> {
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
    public void queries_with_no_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.update("INSERT INTO testtable (first) VALUES (?)",
                    batch(bind(10)),
                    batch(bind(11)));
        });

        ArrayList<Integer> result = new ArrayList<>();
        executor.execute(connection -> connection.select("SELECT first FROM testtable ORDER BY first ASC",
                row -> result.add(row.asInteger("first")))
        );

        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(10));
        assertThat(result.get(1), is(11));
    }

    @Test
    public void queries_with_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    batch(bind(10), bind("asecond10")),
                    batch(bind(11), bind("asecond11")));
        });

        List<Row> result = new ArrayList<>();
        executor.execute(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first = ? AND second = ?",
                        result::add,
                        bind(10), bind("asecond10"))
        );

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asInteger("first"), is(10));
        assertThat(result.get(0).asString("second"), is("asecond10"));
    }

    @Test
    public void queries_with_null_integer_values() throws SQLException {

        executor.execute(connection -> {
            connection.update("CREATE TABLE testtable (first INTEGER NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (null, 'asecond')");
        });

        List<Row> result = new ArrayList<>();
        executor.execute(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first is NULL",
                        result::add
                ));

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asInteger("first"), is(nullValue()));
        assertThat(result.get(0).asString("second"), is("asecond"));
    }


    @Test
    public void inserts_null_double_as_bind_values() throws SQLException {

        executor.execute(connection -> {
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
    public void inserts_double_as_bind_values() throws SQLException {

        executor.execute(connection -> {
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

        executor.execute(connection -> {
            connection.update("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)");
            connection.update("INSERT INTO testtable (first, second) VALUES (null, 'asecond')");
        });

        List<Row> result = new ArrayList<>();
        executor.execute(connection ->
                connection.select("SELECT first, second FROM testtable WHERE first is NULL",
                        result::add
                ));

        assertThat(result.size(), is(1));
        assertThat(result.get(0).asDouble("first"), is(nullValue()));
        assertThat(result.get(0).asString("second"), is("asecond"));
    }

    private List<Row> query(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            ArrayList<Row> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new WrappedResultSet(rs));
            }
            connection.commit();
            return result;
        }
    }


}
