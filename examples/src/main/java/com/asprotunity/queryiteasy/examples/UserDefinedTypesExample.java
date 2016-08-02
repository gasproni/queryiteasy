package com.asprotunity.queryiteasy.examples;

import com.asprotunity.queryiteasy.DataStore;
import com.asprotunity.queryiteasy.connection.InputParameter;
import com.asprotunity.queryiteasy.exception.RuntimeIOException;
import com.asprotunity.queryiteasy.exception.RuntimeSQLException;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.hsqldb.jdbc.JDBCDataSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;


/**
 * Example of how to write some custom functions to bind and query parameters of user defined types.
 * You need to have an instance of Postgres running to run it.
 */
public class UserDefinedTypesExample {

    public static InputParameter bindSong(Song song) {
        return (statement, position, queryScope) -> {
            try {
                ByteOutputStream baos = queryScope.add(new ByteOutputStream(), ByteOutputStream::close);
                ObjectOutputStream oos = queryScope.add(new ObjectOutputStream(baos), ObjectOutputStream::close);
                oos.writeObject(song);
                oos.flush();
                RuntimeSQLException.execute(() -> statement.setBytes(position, baos.getBytes()));
            } catch (IOException exception) {
                throw new RuntimeIOException(exception);
            }
        };
    }

    public static Song asSong(ResultSet resultSet, String columnLabel) {
        return RuntimeSQLException.executeWithResult(() -> {
            try (ByteArrayInputStream in = new ByteArrayInputStream(resultSet.getBytes(columnLabel));
                 ObjectInputStream is = new ObjectInputStream(in)) {
                return (Song) is.readObject();
            } catch (IOException exception) {
                throw new RuntimeIOException(exception);
            } catch (ClassNotFoundException exception) {
                throw new RuntimeException(exception);
            }
        });
    }

    public static void main(String[] args) {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:testdb");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        DataStore dataStore = new DataStore(dataSource);

        Song song = new Song("this is the title", "this is the band", 1988);
        dataStore.execute(connection -> {
            connection.update("CREATE TABLE SongCollection (song BLOB NOT NULL)");

            connection.update("INSERT INTO SongCollection (song) VALUES (?)", bindSong(song));
        });

        dataStore.execute(connection -> connection.select(resultSet -> asSong(resultSet, "song"),
                                                          "SELECT song FROM SongCollection")
                .forEach(System.out::println));

    }

}
