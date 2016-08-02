package com.asprotunity.queryiteasy.examples;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGpoint;

import java.sql.ResultSet;

import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;


/**
 * Example of how to write some custom functions to bind and query parameters of custom database types.
 * You need to have an instance of Postgres running to run it.
 */
public class CustomPostgresTypesExample {

    public static InputParameter bindCircle(PGcircle circle) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setObject(position, circle));
    }

    public static PGcircle asCircle(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> (PGcircle) resultSet.getObject(columnLabel));
    }

    public static void main(String[] args) {
        // Before compiling put the information corresponding to your Postgres setup.
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerName("localhost");
        dataSource.setDatabaseName("testdb");
        String user = "testuser";
        dataSource.setUser(user);
        dataSource.setPassword("testpassword");

        DataStore dataStore = new DataStore(dataSource);
        try {
            dataStore.execute(connection -> connection.update("CREATE TABLE geomtest (mycircle circle)"));

            PGpoint center = new PGpoint(1, 2.5);
            double radius = 4;
            PGcircle circle = new PGcircle(center, radius);

            dataStore.execute(connection -> connection.update("INSERT INTO geomtest (mycircle) VALUES (?)",
                                                              bindCircle(circle)));

            dataStore.execute(
                    connection -> connection.select(resultSet -> asCircle(resultSet, "mycircle"),
                                                    "SELECT mycircle FROM geomtest")
                            .forEach(c -> {
                                System.out.println(String.format("Center (x, y): (%f, %f) ", c.center.x, c.center.y));
                                System.out.println(String.format("Radius: %f ", c.radius));
                            })
            );


        } finally {
            // remove the newly created table and leave everything clean.
            dataStore.execute(
                    connection -> connection.select(rs -> asString(rs, 1),
                                                    "select 'drop table if exists \"' || tablename || '\" cascade;'" +
                                                            "  from pg_tables " +
                                                            " where tableowner = ?", bind(user))
                            .forEach(statement -> connection.update(statement)));
        }

    }

}
