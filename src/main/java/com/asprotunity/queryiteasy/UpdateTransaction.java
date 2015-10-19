package com.asprotunity.queryiteasy;


@FunctionalInterface
public interface UpdateTransaction {
    void execute(Connection connection);
}
