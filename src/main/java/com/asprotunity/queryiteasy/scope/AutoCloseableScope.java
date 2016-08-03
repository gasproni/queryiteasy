package com.asprotunity.queryiteasy.scope;

public interface AutoCloseableScope extends Scope, AutoCloseable {

    /**
     * Removes the throws clause.
     */
    @Override
    void close();
}
