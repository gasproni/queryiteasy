Queryiteasy is a wrapper around the Java JDBC API to reduce the amount of boilerplate code as well as the occurrence of
programmer errors. It is written in Java 8.

Some very important rules are:
 
 * null is never a valid parameter in any of the API calls, with the only exception of the StatementParameter::bind methods as those may need to receive null values to store in the database.
 * null is never returned except for the asXXX methods in the Row class, which can return null database values.
 * There are no commits, rollbacks or close. Transactions are automatically committed when they terminate successfully, and automatically rolled back in case of exceptions.
 
To compile execute the "gradlew build" (or "gradlew.bat build" if in Windows) script. That will download the necessary
gradle packages, compile the project and run all the tests.

To see at example usages have a look at the acceptance test classes in [src/acceptanceTest/java/com/asprotunity/queryiteasy/acceptance_tests](src/acceptanceTest/java/com/asprotunity/queryiteasy/acceptance_tests).