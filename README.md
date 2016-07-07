# Queryiteasy: a Java 8 wrapper to make JDBC easier to use #

The JDBC API is quite powerful and efficient, but it can be quite cumbersome to use—the required amount of boilerplate
code can make for code difficult to read and error prone. Queryiteasy aims at solving this problem. Here is a motivating example.

Suppose we have a database with the following "Song" table with the following columns:

|Name  |SQL Type|
|:-----|:-------|
|Title |VARCHAR |
|Band  |VARCHAR |
|Year  |INTEGER |

To query all songs from "Rolling Stones" published in 1975 we could do the following:

With JDBC:
```java
DataSource dataSource = ...;
try (Connection connection = dataSource.getConnection();
     PreparedStatement statement = connection.prepareStatement("SELECT title FROM song WHERE band = ? and year = ?")) {
     statement.setString(1, "Rolling Stones");
     statement.setInt(2, 1975);
     try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
            // process result set
        }
     }
     
} catch (SQLException e) {
   // This is checked so either you manage it or declare it in 
   // the throws clause of the surrounding method.
}
```

With Queryiteasy:
```java
DataSource dataSource = ...;
// The dataStore is created once and passed around in your code.
Datastore dataStore = new DataStore(dataSource);
dataStore.execute(connection -> connection.select("SELECT title FROM song WHERE band = ? and year = ?", 
                                                  rowProcessor, 
                                                  bind("Rolling Stones"), bind(1975)));

```

`rowProcessor` in the code above is a function that will process the results of the select—basically doing what the while loop does in the JDBC example.
The method `Datastore.execute` defines also the transaction boundary—f any calls inside the lambda throw an exception the transaction will be rolled back, otherwise, if everything goes well, it will be committed.
The SQLException checked exception has been wrapped in a runtime exception, RuntimeSQLException, so it won't interfere with the use of lambdas.

To see other examples, have a look at the acceptance test classes in 
[src/acceptanceTest/java/com/asprotunity/queryiteasy/acceptance_tests](src/acceptanceTest/java/com/asprotunity/queryiteasy/acceptance_tests).
 
To compile execute the "gradlew build" (or "gradlew.bat build" if in Windows) script. That will download the necessary
gradle packages, compile the project and run all the tests.

