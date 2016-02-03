package com.asprotunity.tersql.acceptance_tests;

import com.asprotunity.tersql.DataStore;
import com.asprotunity.tersql.connection.Row;
import com.asprotunity.tersql.internal.RowFromResultSet;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class EndToEndTestBase {
    protected static JDBCDataSource dataSource;
    protected static DataStore dataStore;

    @BeforeClass
    public static void setUp() {
        EndToEndTestBase.dataSource = new JDBCDataSource();
        EndToEndTestBase.dataSource.setDatabase("jdbc:hsqldb:mem:testdb");
        EndToEndTestBase.dataSource.setUser("sa");
        EndToEndTestBase.dataSource.setPassword("");
        EndToEndTestBase.dataStore = new DataStore(EndToEndTestBase.dataSource);
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

    protected List<Row> query(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            ArrayList<Row> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new RowFromResultSet(rs));
            }
            connection.commit();
            return result;
        }
    }

    protected void prepareExpectedData(String firstSqlStatement, String... otherSqlStatements) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(firstSqlStatement);
            for (String sql : otherSqlStatements) {
                statement.execute(sql);
            }
            connection.commit();
        }
    }
}
