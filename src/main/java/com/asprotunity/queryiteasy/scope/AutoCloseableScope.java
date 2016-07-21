package com.asprotunity.queryiteasy.scope;

public interface AutoCloseableScope extends Scope, AutoCloseable {

    /**
     * Removes the throw clause.
     */
    @Override
    void close();
}
