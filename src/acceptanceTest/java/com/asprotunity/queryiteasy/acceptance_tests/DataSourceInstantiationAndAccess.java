package com.asprotunity.queryiteasy.acceptance_tests;

import com.asprotunity.queryiteasy.scope.AutoCloseableScope;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public interface DataSourceInstantiationAndAccess {
    static DataSource instantiateDataSource(String fullyQualifiedClassName)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class<?> clazz = Class.forName(fullyQualifiedClassName);
        return (DataSource) clazz.newInstance();
    }

    static List<FlexibleTuple> query(DataSource dataSource, String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             AutoCloseableScope connectionScope = new AutoCloseableScope();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            ArrayList<FlexibleTuple> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new FlexibleTupleFromResultSet(rs));
            }
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    static void prepareData(DataSource dataSource, String firstSqlStatement, String... otherSqlStatements) throws SQLException {
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
