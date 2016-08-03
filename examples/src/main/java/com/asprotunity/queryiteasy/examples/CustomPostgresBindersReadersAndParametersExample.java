package com.asprotunity.queryiteasy.examples;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.InputOutputParameter;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.connection.OutputParameter;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.asprotunity.queryiteasy.scope.Scope;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.geometric.PGcircle;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Types;


/**
 * Example of how to write some custom functions to bind and query parameters of custom database types.
 * You need to have an instance of Postgres running to run it.
 */
public class CustomPostgresBindersReadersAndParametersExample {

    /**
     * Example of custom binder function.
     */
    public static InputParameter bindCircle(PGcircle circle) {
        return (statement, position, queryScope) ->
                RuntimeSQLException.execute(() -> statement.setObject(position, circle));
    }

    /**
     * Example of custom result set reader function.
     */
    public static PGcircle asCircle(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> (PGcircle) resultSet.getObject(columnLabel));
    }

    /**
     * Example of custom output parameter to use in a stored procedure.
     */
    public static class PGcircleOutputParameter implements OutputParameter {
        private PGcircle value = null;

        public PGcircle value() {
            return value;
        }

        @Override
        public void bind(CallableStatement statement, int position, Scope queryScope) {
            RuntimeSQLException.execute(() -> {
                // Register the output parameter before calling the procedure
                statement.registerOutParameter(position, Types.OTHER);
                // Read the value of the output parameter after the call to the procedure returns
                queryScope.add(() -> this.value = (PGcircle) statement.getObject(position));
            });
        }
    }

    /**
     * Example of custom input-output parameter to use in a stored procedure.
     */
    public static class PGcircleInputOutputParameter implements InputOutputParameter {
        private PGcircle value;

        PGcircleInputOutputParameter(PGcircle value) {
            this.value = value;
        }

        public PGcircle value() {
            return value;
        }

        @Override
        public void bind(CallableStatement statement, int position, Scope queryScope) {
            RuntimeSQLException.execute(() -> {
                // Set the input value for the procedure
                statement.setObject(position, value);
                // Register the output parameter before calling the procedure
                statement.registerOutParameter(position, Types.OTHER);
                // Read the value of the output parameter after the call to the procedure returns
                queryScope.add(() -> this.value = (PGcircle) statement.getObject(position));
            });
        }
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

        System.out.println("-----------------------Begin exampleOfQueryWithCustomBinderAndReader Example-----------------------");
        exampleOfQueryWithCustomBinderAndReader(dataStore, user);
        System.out.println("-----------------------End exampleOfQueryWithCustomBinderAndReader Example-----------------------");
        System.out.println("-----------------------Begin exampleOfCustomOutputAndInputOutputParameterUse Example-----------------------");
        exampleOfCustomOutputAndInputOutputParameterUse(dataStore);
        System.out.println("-----------------------End exampleOfCustomOutputAndInputOutputParameterUse Example-----------------------");


    }

    private static void exampleOfQueryWithCustomBinderAndReader(DataStore dataStore, String user) {
        try {
            dataStore.execute(connection -> connection.update("CREATE TABLE geomtest (mycircle circle)"));

            dataStore.execute(connection -> connection.update("INSERT INTO geomtest (mycircle) VALUES (?)",
                                                              bindCircle(new PGcircle(1, 2.5, 4))));

            dataStore.execute(
                    connection -> connection.select(resultSet -> asCircle(resultSet, "mycircle"),
                                                    "SELECT mycircle FROM geomtest")
                            .forEach(c -> {
                                System.out.println(String.format("Center (x, y): (%f, %f) ", c.center.x, c.center.y));
                                System.out.println(String.format("Radius: %f ", c.radius));
                            })
            );
        } finally {
            dataStore.execute(connection -> connection.update("DROP TABLE geomtest;"));
        }
    }

    private static void exampleOfCustomOutputAndInputOutputParameterUse(DataStore dataStore) {

        try {
            dataStore.execute(connection ->
                                      connection.update("CREATE FUNCTION swap_input_and_io_values(in inparam circle, \n" +
                                                                "    OUT outparam circle, INOUT inoutparam circle)\n" +
                                                                "   AS\n" +
                                                                "$$\n" +
                                                                "BEGIN\n" +
                                                                "    outparam := inoutparam;\n" +
                                                                "    inoutparam := inparam;\n" +
                                                                "END;\n" +
                                                                "    $$\n" +
                                                                "  LANGUAGE 'plpgsql' VOLATILE;"));

            PGcircle inparamValue = new PGcircle(1, 2, 3);
            PGcircle initialInoutparamValue = new PGcircle(4, 5, 6);
            PGcircleOutputParameter outparam = new PGcircleOutputParameter();
            PGcircleInputOutputParameter inoutparam = new PGcircleInputOutputParameter(initialInoutparamValue);

            dataStore.execute(connection -> connection.call("{call swap_input_and_io_values(?, ?, ?)}",
                                                            bindCircle(inparamValue), outparam, inoutparam));

            System.out.println(String.format("Input param (x, y, r): (%f, %f, %f)",
                                             inparamValue.center.x, inparamValue.center.y, inparamValue.radius));
            System.out.println(String.format("Out param final (x, y, r): (%f, %f, %f)",
                                             outparam.value().center.x, outparam.value().center.y, outparam.value().radius));
            System.out.println(String.format("IO param initial (x, y, r): (%f, %f, %f)",
                                             initialInoutparamValue.center.x, initialInoutparamValue.center.y, initialInoutparamValue.radius));
            System.out.println(String.format("IO param final (x, y, r): (%f, %f, %f)",
                                             inoutparam.value().center.x, inoutparam.value().center.y, inoutparam.value().radius));

        } finally {
            dataStore.execute(connection -> connection.update("DROP FUNCTION swap_input_and_io_values(circle, out circle, inout circle)"));
        }
    }

}
