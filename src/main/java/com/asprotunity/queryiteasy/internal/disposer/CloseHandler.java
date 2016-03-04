package com.asprotunity.queryiteasy.internal.disposer;

@FunctionalInterface
public interface CloseHandler {
    void apply() throws Exception;
}
