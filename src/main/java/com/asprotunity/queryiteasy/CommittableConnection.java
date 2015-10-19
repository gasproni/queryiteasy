package com.asprotunity.queryiteasy;

/**
 * Created by giovanni on 19/10/2015.
 */
public interface CommittableConnection extends Connection, AutoCloseable {
    void commit();
    void close();
}
