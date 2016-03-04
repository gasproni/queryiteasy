package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.Row;
import com.asprotunity.queryiteasy.internal.connection.RowFromResultSet;
import org.junit.After;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EndToEndTestBase {

    public static final String QUERYITEASY_DATASOURCE_CLASS = "queryiteasy.datasource.class";
    public static final String QUERYITEASY_DATASOURCE_URL = "queryiteasy.datasource.url";
    public static final String QUERYITEASY_DATASOURCE_USER = "queryiteasy.datasource.user";
    public static final String QUERYITEASY_DATASOURCE_PASSWORD = "queryiteasy.datasource.password";
    public static final String QUERYITEASY_TEST_CONFIG_FILE = "queryiteasy.test.config.file";

    private static DataSource dataSource;
    private static DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {

        Properties testConfigProperties = loadTestConfigProperties();

        dataSource = createDataSource(testConfigProperties);
        dataStore = new DataStore(dataSource);
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

    protected static DataStore getDataStore() {
        return dataStore;
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

    private static DataSource createDataSource(Properties testConfigProperties) throws Exception {
        Class<?> clazz = Class.forName(testConfigProperties.getProperty(QUERYITEASY_DATASOURCE_CLASS));
        DataSource result = (DataSource) clazz.newInstance();

        Method setUrl = clazz.getMethod("setURL", String.class);
        setUrl.invoke(result, testConfigProperties.getProperty(QUERYITEASY_DATASOURCE_URL));

        Method setUser = clazz.getMethod("setUser", String.class);
        setUser.invoke(result, testConfigProperties.getProperty(QUERYITEASY_DATASOURCE_USER));

        Method setPassword = clazz.getMethod("setPassword", String.class);
        setPassword.invoke(result, testConfigProperties.getProperty(QUERYITEASY_DATASOURCE_PASSWORD));
        return result;

    }

    private static Properties loadTestConfigProperties() throws IOException {
        String configFilePath = System.getProperty(QUERYITEASY_TEST_CONFIG_FILE);

        Properties result = new Properties();
        if (configFilePath != null) {
            try (FileInputStream in = new FileInputStream(configFilePath)) {
                result.load(in);
                in.close();
            }
        } else {
            result.put(QUERYITEASY_DATASOURCE_CLASS, "org.hsqldb.jdbc.JDBCDataSource");
            result.put(QUERYITEASY_DATASOURCE_URL, "jdbc:hsqldb:mem:testdb");
            result.put(QUERYITEASY_DATASOURCE_USER, "sa");
            result.put(QUERYITEASY_DATASOURCE_PASSWORD, "");
        }
        return result;
    }

}
