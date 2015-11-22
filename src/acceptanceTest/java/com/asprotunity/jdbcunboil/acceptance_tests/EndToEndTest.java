package com.asprotunity.jdbcunboil.acceptance_tests;


import com.asprotunity.jdbcunboil.TransactionExecutor;
import com.asprotunity.jdbcunboil.connection.Batch;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.asprotunity.jdbcunboil.connection.Batch.batch;
import static com.asprotunity.jdbcunboil.connection.StatementParameter.bind;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public class EndToEndTest {

    private JDBCDataSource dataSource;
    private TransactionExecutor executor;

    @Before
    public void setUp() {
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
        statement.execute("SHUTDOWN");
        statement.close();
        connection.commit();
        connection.close();
    }


    @Test
    public void inserts_with_no_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first) VALUES (10)");
        });

        List<Integer> expectedValues = query("SELECT first FROM testtable", rs -> rs.getInt("first"));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0), is(10));
    }


    @Test
    public void inserts_with_some_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(10), bind("asecond"));
        });

        List<TestTableFields<Integer, String>> expectedValues = query("SELECT * FROM testtable",
                rs -> make(getInteger(rs, "first"), getString(rs, "second")));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).first, is(10));
        assertThat(expectedValues.get(0).second, is("asecond"));
    }

    @Test
    public void inserts_null_integers_as_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind((Integer)null), bind("asecond"));
        });

        List<TestTableFields> expectedValues = query("SELECT * FROM testtable",
                rs -> make(getInteger(rs, "first"), getString(rs, "second")));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).first, is(nullValue()));
        assertThat(expectedValues.get(0).second, is("asecond"));
    }


    @Test
    public void can_do_batch_inserts() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    batch(bind(10), bind("asecond10")),
                    batch(bind(11), bind("asecond11")),
                    batch(bind(12), bind("asecond12")));
        });

        List<TestTableFields> expectedValues = query("SELECT * FROM testtable ORDER BY first ASC",
                rs -> make(getInteger(rs, "first"), getString(rs, "second")));
        assertThat(expectedValues.size(), is(3));
        for (int first = 0; first < expectedValues.size(); ++first) {
            assertThat(expectedValues.get(first).first, is(first + 10));
            assertThat(expectedValues.get(first).second, is("asecond" + (first + 10)));
        }
    }

    @Test
    public void can_do_batch_inserts_with_batch_array() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");

            Batch firstBatch = batch(bind(10), bind("asecond10"));
            Batch[] batches = new Batch[2];
            for (int first = 0; first < 2; ++first) {
                int value = first + 11;
                batches[first] = batch(bind(value), bind("asecond" + value));
            }

            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)", firstBatch, batches);
        });

        List<TestTableFields> expectedValues = query("SELECT * FROM testtable ORDER BY first ASC",
                rs -> make(getInteger(rs, "first"), getString(rs, "second")));
        assertThat(expectedValues.size(), is(3));
        for (int first = 10; first < expectedValues.size(); ++first) {
            assertThat(expectedValues.get(first).first, is(first));
            assertThat(expectedValues.get(first).second, is("asecond" + first));
        }
    }


    @Test
    public void can_query_with_no_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first) VALUES (?)",
                    batch(bind(10)),
                    batch(bind(11)));
        });

        List<Integer> result = executor.executeQuery(connection ->
                        connection.executeQuery("SELECT first FROM testtable ORDER BY first ASC",
                                row -> row.getInteger("first"))
        );


        assertThat(result.size(), is(2));
        assertThat(result.get(0), is(10));
        assertThat(result.get(1), is(11));
    }

    @Test
    public void can_query_with_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NOT NULL, second VARCHAR(20) NOT NULL)");

            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    batch(bind(10), bind("asecond10")),
                    batch(bind(11), bind("asecond11")));
        });

        List<TestTableFields> result = executor.executeQuery(connection ->
                        connection.executeQuery("SELECT first, second FROM testtable WHERE first = ? AND second = ?",
                                row -> make(row.getInteger("first"), row.getString("second")),
                                bind(10),
                                bind("asecond10"))
        );

        assertThat(result.size(), is(1));
        assertThat(result.get(0).first, is(10));
        assertThat(result.get(0).second, is("asecond10"));
    }

    @Test
    public void can_query_null_integer_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first INTEGER NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (null, 'asecond')");
        });

        List<TestTableFields> result = executor.executeQuery(connection ->
                        connection.executeQuery("SELECT first, second FROM testtable WHERE first is NULL",
                                row -> make(row.getInteger("first"), row.getString("second")))
        );

        assertThat(result.size(), is(1));
        assertThat(result.get(0).first, is(nullValue()));
        assertThat(result.get(0).second, is("asecond"));
    }


    @Test
    public void inserts_null_double_as_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind((Double)null), bind("asecond"));
        });

        List<TestTableFields> expectedValues = query("SELECT * FROM testtable",
                rs -> make(getDouble(rs, "first"), getString(rs, "second")));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).first, is(nullValue()));
        assertThat(expectedValues.get(0).second, is("asecond"));
    }

    @Test
    public void inserts_double_as_bind_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (?, ?)",
                    bind(1.7d), bind("asecond"));
        });

        List<TestTableFields<Double, String>> expectedValues = query("SELECT * FROM testtable",
                rs ->make(getDouble(rs, "first"), getString(rs, "second")));
        assertThat(expectedValues.size(), is(1));
        assertThat(expectedValues.get(0).first, is(closeTo(1.7d, 0.00000000001d)));
        assertThat(expectedValues.get(0).second, is("asecond"));
    }

    @Test
    public void can_query_null_double_values() throws SQLException {

        executor.execute(connection -> {
            connection.executeUpdate("CREATE TABLE testtable (first DOUBLE NULL, second VARCHAR(20) NOT NULL)");
            connection.executeUpdate("INSERT INTO testtable (first, second) VALUES (null, 'asecond')");
        });

        List<TestTableFields> result = executor.executeQuery(connection ->
                connection.executeQuery("SELECT first, second FROM testtable WHERE first is NULL",
                        row -> make(row.getDouble("first"), row.getString("second")))
        );

        assertThat(result.size(), is(1));
        assertThat(result.get(0).first, is(nullValue()));
        assertThat(result.get(0).second, is("asecond"));
    }

    private <ResultType> List<ResultType> query(String sql, ResultSetMapper<ResultType> mapper) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            ArrayList<ResultType> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapper.apply(rs));
            }
            connection.commit();
            return result;
        }
    }


    private static class TestTableFields<Type1, Type2> {
        public final Type1 first;
        public final Type2 second;

        public TestTableFields(Type1 first, Type2 second) {
            this.first = first;
            this.second = second;
        }

    }

    private static <Type1, Type2 > TestTableFields<Type1, Type2> make(Type1 val1, Type2 val2) {
        return new TestTableFields<>(val1, val2);
    }

    @FunctionalInterface
    interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }


    private Double getDouble(ResultSet rs, String columnName) throws SQLException {
        double result = rs.getDouble(columnName);
        if (rs.wasNull()) {
            return null;
        }
        return result;
    }

    private Integer getInteger(ResultSet rs, String columnName) throws SQLException {
        int result = rs.getInt(columnName);
        if (rs.wasNull()) {
            return null;
        }
        return result;
    }

    private String getString(ResultSet rs, String columnName) throws SQLException {
        return rs.getString(columnName);
    }

}
