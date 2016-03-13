package com.asprotunity.queryiteasy.closer;

@FunctionalInterface
public interface CloseAction {
    void perform() throws Exception;
}
