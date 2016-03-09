package com.asprotunity.queryiteasy.disposer;

@FunctionalInterface
public interface CloseAction {
    void perform() throws Exception;
}
