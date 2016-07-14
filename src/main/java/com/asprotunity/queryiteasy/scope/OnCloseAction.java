package com.asprotunity.queryiteasy.scope;

@FunctionalInterface
public interface OnCloseAction {
    void perform() throws Exception;
}
