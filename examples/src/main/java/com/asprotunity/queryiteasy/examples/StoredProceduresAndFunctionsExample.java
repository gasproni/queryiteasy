package com.asprotunity.queryiteasy.examples;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.IntegerInputOutputParameter;
import com.asprotunity.queryiteasy.connection.StringOutputParameter;
import org.hsqldb.jdbc.JDBCDataSource;

import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asInteger;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;
import static java.util.Arrays.asList;

public class StoredProceduresAndFunctionsExample {
    public static void main(String[] args) {

        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:testdb");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        DataStore dataStore = new DataStore(dataSource);

        dataStore.execute(connection -> { // The transaction starts here
            // Null year means unknown
            connection.update("CREATE TABLE song (title VARCHAR(20) NOT NULL, band VARCHAR(100) NOT NULL, year INTEGER NULL)");

            // Do a batch insert.
            connection.update("INSERT INTO song (title, band, year) VALUES (?, ?, ?)",
                              asList(batch(bind("Smoke on the Water"), bind("Deep Purple"), bind(1973)),
                                     batch(bind("I Got the Blues"), bind("Rolling Stones"), bind((Integer) null)),
                                     batch(bind("Hey Jude"), bind("Beatles"), bind(1968))));

        }); // The transaction ends here. It commits (or rolls back, in case of errors) automatically.


        System.out.println("-----------------------Begin Stored Procedure Example-----------------------");

        storedProcedureOutputAndIOParametersExample(dataStore);

        System.out.println("-----------------------End Stored Procedure Example-----------------------");

        System.out.println("-----------------------Begin Stored Function Example-----------------------");

        storedFunctionExample(dataStore);

        System.out.println("-----------------------End Stored Function Example-----------------------");


    }

    private static void storedProcedureOutputAndIOParametersExample(DataStore dataStore) {
        dataStore.execute(
                connection ->
                        connection.update("CREATE PROCEDURE return_band_and_change_and_return_year(in ptitle VARCHAR(20), " +
                                                  "                                                out pband VARCHAR(100), " +
                                                  "                                                inout pyear INTEGER)\n" +
                                                  "MODIFIES SQL DATA\n" +
                                                  "BEGIN ATOMIC \n" +
                                                  "   DECLARE temp_year INTEGER;\n" +
                                                  "   SET temp_year = pyear;\n" +
                                                  "   SELECT band, year into pband, pyear FROM song WHERE title = ptitle;\n" +
                                                  "   UPDATE song SET year = temp_year WHERE title = ptitle;\n" +
                                                  " END")
        );

        // Use of input and input-output parameters. they can be declared and used inside or outside a transaction.
        StringOutputParameter bandNameOutParam = new StringOutputParameter();
        IntegerInputOutputParameter yearInOutParam = new IntegerInputOutputParameter(2016);
        String title = "Hey Jude";
        dataStore.execute(connection -> connection.call("{call return_band_and_change_and_return_year(?, ?, ?)}",
                                                        bind(title), bandNameOutParam, yearInOutParam));

        Integer newYearValue = dataStore.executeWithResult(
                connection -> connection.select(resultSet -> asInteger(resultSet, "year"),
                                                "SELECT year FROM song WHERE title = ?",
                                                bind(title)).findFirst().orElse(null)
        );

        System.out.println("Title: " + title);
        System.out.println("Band: " + bandNameOutParam.value());
        System.out.println("Previous year value: " + yearInOutParam.value());
        System.out.println("New year value: " + newYearValue);
    }

    private static void storedFunctionExample(DataStore dataStore) {
        dataStore.execute(connection -> {
            connection.update("CREATE FUNCTION query_band_by_title(in ptitle VARCHAR(20))\n" +
                                      "RETURNS TABLE(title VARCHAR(20), band VARCHAR(100), yr INTEGER)\n" +
                                      "READS SQL DATA BEGIN ATOMIC\n" +
                                      "RETURN TABLE(SELECT title, band, year FROM song WHERE title = ptitle);\n" +
                                      "END");

            String title = "Smoke on the Water";
            connection.call(resultSet -> new Song(asString(resultSet, "title"),
                                                  asString(resultSet, "band"),
                                                  asInteger(resultSet, "year")),
                            "{call query_band_by_title(?)}",
                            bind(title))
                    .forEach(song -> {
                        System.out.println("Title: " + song.title);
                        System.out.println("Band: " + song.band);
                        System.out.println("Year: " + song.year);
                    });

        });

    }
}
