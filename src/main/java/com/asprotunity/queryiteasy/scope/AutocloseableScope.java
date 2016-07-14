package com.asprotunity.queryiteasy.scope;

import java.util.ArrayList;


public class AutoCloseableScope implements AutoCloseable, Scope {

    private boolean isClosed = false;
    private ArrayList<OnCloseAction> onCloseActions = new ArrayList<>();

    @Override
    public void add(OnCloseAction caller) {
        this.onCloseActions.add(caller);
    }

    @Override
    public <RType> RType add(RType obj, ThrowingConsumer<RType> consumer) {
        add(() -> consumer.apply(obj));
        return obj;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        ScopeException toThrow = null;
        for (int position = onCloseActions.size() - 1; position >= 0; --position) {
            try {
                onCloseActions.get(position).perform();
            } catch (Exception exception) {
                if (toThrow == null) {
                    toThrow = new ScopeException(exception);
                } else {
                    toThrow.addSuppressed(exception);
                }
            }
        }
        isClosed = true;
        if (toThrow != null) {
            throw toThrow;
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    public int handlersCount() {
        return onCloseActions.size();
    }

}
