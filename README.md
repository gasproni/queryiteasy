# Queryiteasy: a Java 8 wrapper to make JDBC easier to use

Queryiteasy makes the use of JDBC less verbose, and easier to use and maintain, by using the functional programming constructs in Java 8.

Here is an example, if we have the table below:

|Name  |SQL Type|
|:-----|:-------|
|Title |VARCHAR |
|Band  |VARCHAR |
|Year  |INTEGER |

To print all song titles from "Rolling Stones" published in 1975 we could do the following:

With JDBC:
```java
DataSource dataSource = ...;
try (Connection connection = dataSource.getConnection();
     PreparedStatement statement = 
            connection.prepareStatement("SELECT title FROM song WHERE band = ? and year = ?")) {
     statement.setString(1, "Rolling Stones");
     statement.setInt(2, 1975);
     try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
            System.out.println(rs.getString("title"))
        }
     }
     
} catch (SQLException e) {
   // Do something with the exception.
}
```

With Queryiteasy:
```java
DataSource dataSource = ...;
// The dataStore is created once and passed around in your code.
Datastore dataStore = new DataStore(dataSource);
dataStore.execute(connection -> 
       connection.select(resultSet->asString(resultSet, "title"),
                         "SELECT title FROM song WHERE band = ? and year = ?", 
                         bindString("Rolling Stones"), bindInteger(1975)).forEach(System.out::println)
);
```

Things to notice:

* The indexed parameters are bound by the `bindString` and `bindInteger` methods in the call itself, making it easier to spot potential mistakes
* SQLException is wrapped in the unchecked [com.asprotunity.queryiteasy.exception.RuntimeSQLException](src/acceptanceTest/java/com/asprotunity/queryiteasy/exception/RuntimeSQLException.java), removing the need for lots of unnecessary try-catch blocks and throws clauses
* The method `Datastore::execute` defines the transaction boundary—if any call inside the lambda passed as parameter throws an exception the transaction will be rolled back, otherwise, if everything goes well, it will be committed, eliminating the need for explicit commit and rollbacks
* The connection is always closed automatically at the end of `execute`

In addition to calls to `connection`, you can execute any other code you want inside the transaction. Here is another example:
```java
dataStore.execute(connection -> { // The transaction starts here
    System.out.println("Starting the transaction!")
    
    // Do a batch insert.
    connection.update("INSERT INTO song (title, band, year) VALUES (?, ?, ?)",
                      asList(batch(bindString("Smoke on the Water"), bindString("Deep Purple"), bindInteger(1973)),
                             batch(bindString("I Got the Blues"), bindString("Rolling Stones"), bindInteger(null)),
                             batch(bindString("Hey Jude"), bindString("Beatles"), bindInteger(1968))));

    System.out.println("About to commit the transaction!")
}); // The transaction ends here. It commits (or rolls back, in case of errors) automatically.
```

[Here are some more examples](src/main/java/com/asprotunity/queryiteasy/examples), or you can also have a look at the acceptance tests 
[here](src/acceptanceTest/java/com/asprotunity/queryiteasy/acceptance_tests/QueriesTest.java).

## Main features ##

* Almost no boilerplate code—e.g., connections, statements and result sets are closed automatically, no need for explicit call to commit or rollback.
* Transactions boundaries clearly visible in code
* Wraps SQLException with the unchecked exception [com.asprotunity.queryiteasy.exception.RuntimeSQLException](src/acceptanceTest/java/com/asprotunity/queryiteasy/exception/RuntimeSQLException.java), removing the need for lots of unnecessary try-catch blocks and throws clauses
* Supports input, output and input-output parameters for queries (input only), and stored procedures and functions, in a clean and consistent way
* Allows for easy customizations to support vendor specific SQL types
* No special configuration—just put the jar in the classpath.
* No dependencies on external libraries and frameworks


## Building the and using the library
To compile call `gradlew build` (or `gradlew.bat build` if in Windows) from the root folder. That will download the necessary
gradle packages, compile the project and run all the tests (except for the (non essential) ones requiring MySQL, Postgres, or Oracle, which require some specific configuration, which I still need to document). The jar will be put in the `build/libs` folder.
To use it, you just need to copy the jar anywhere you like and put it in your classpath.

calling `gradlew build` will also compile all the examples in the [examples](src/main/java/com/asprotunity/queryiteasy/examples) folder.

# Getting started

The main core classes for the library are:

* [com.asprotunity.queryiteasy.datastore.DataStore](src/main/java/com/asprotunity/queryiteasy/datastore/DataStore.java), which has the methods to run transactions
* [com.asprotunity.queryiteasy.connection.Connection](src/main/java/com/asprotunity/queryiteasy/connection/Connection.java), which has the methods to execute database queries
* [com.asprotunity.queryiteasy.connection.InputParameterBinders](src/main/java/com/asprotunity/queryiteasy/connection/InputParameterBinders.java), which has the functions to bind positional parameters to queries
* [com.asprotunity.queryiteasy.connection.ResultSetReaders](src/main/java/com/asprotunity/queryiteasy/connection/ResultSetReaders.java), which has the functions to read `ResultSets`

Have a look at the examples [here](src/main/java/com/asprotunity/queryiteasy/examples/) to see how to use them. 

All the classes named `<XXX>InputOutputParameter` and `<XXX>OutputParameter` implement the functionality to support output and input-output parameters for stored functions and procedures. Have a look [here](src/main/java/com/asprotunity/queryiteasy/examples/StoredProceduresAndFunctionsExample.java) for some usage examples.

## Supporting custom database types

Until I write some better documentation, have a look at [this example](src/main/java/com/asprotunity/queryiteasy/examples/CustomPostgresBindersReadersAndParametersExample.java) to see how to support custom database types.

## Databases used for testing

Queryiteasy doesn't depend only on JDBC, not on any specific database, but it has been tested with the following ones to spot potential issues (each of them comes with its own quirks):

 * HSQLDB 2.3.3
 * MySQL 5.7.10
 * Postgres 9.5.1.0
 * Oracle 12.1.0.2

## Credits ##

[Rafal Ganczarek](https://github.com/ganczarek) provided a lot of useful feedback and great design suggestions.

