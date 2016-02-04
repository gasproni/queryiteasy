package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.internal.RowFromResultSet;
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

    private static JDBCDataSource dataSource;
    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() {
        dataSource = new JDBCDataSource();
        dataSource.setDatabase("jdbc:hsqldb:mem:testdb");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        EndToEndTestBase.dataStore = new DataStore(dataSource);
    }

    protected static DataStore getDataStore() {
        return dataStore;
    }

    @After
    public void tearDown() throws SQLException {
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE testtable");
        statement.close();
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
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
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
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
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }
}
