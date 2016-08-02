package com.asprotunity.queryiteasy.examples;

import com.asprotunity.queryiteasy.DataStore;
import org.hsqldb.jdbc.JDBCDataSource;

import java.util.List;

import static com.asprotunity.queryiteasy.connection.Batch.batch;
import static com.asprotunity.queryiteasy.connection.InputParameterBinders.bind;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asInteger;
import static com.asprotunity.queryiteasy.connection.ResultSetReaders.asString;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;


public class QueriesExample {

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


        // Query songs after 1968
        dataStore.execute(connection -> {
            int year = 1968;
            System.out.println("Songs after " + year + ": ");
            connection.select(resultSet -> new Song(asString(resultSet, "title"),
                                                    asString(resultSet, "band"),
                                                    asInteger(resultSet, 3)),
                              "SELECT title, band, year from song WHERE year > ?", bind(year))
                    .forEach(song -> System.out.println(String.join(" ", song.title, song.band, String.valueOf(song.year))));
        });


        // Return result from transaction: All songs with year set to null.
        List<Song> result = dataStore.executeWithResult(
                connection ->
                        connection.select(resultSet -> new Song(asString(resultSet, "title"),
                                                                asString(resultSet, "band"),
                                                                asInteger(resultSet, 3)),
                                          "SELECT title, band, year from song WHERE year IS NULL")
                                .collect(toList())
        );

        // Outside the previous transaction now.
        System.out.println("Songs of unknown year:");
        for (Song song : result) {
            System.out.println(String.join(" ", song.title, song.band, String.valueOf(song.year)));
        }

    }

}
