package com.asprotunity.queryiteasy.disposer;

@FunctionalInterface
public interface CloseHandler {
    void apply() throws Exception;
}
