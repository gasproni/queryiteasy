package com.asprotunity.queryiteasy.connection;

public class RuntimeSQLException extends RuntimeException {

    public RuntimeSQLException(java.sql.SQLException cause) {
        super(cause);
    }

    public RuntimeSQLException(String message) {
        super(message);
    }

}
