package com.asprotunity.queryiteasy.scope;

import java.util.ArrayList;


public class AutoCloseableScope implements AutoCloseable, Scope {

    private boolean isClosed = false;
    private ArrayList<LeaveAction> leaveActions = new ArrayList<>();

    @Override
    public void onLeave(LeaveAction caller) {
        this.leaveActions.add(caller);
    }

    @Override
    public <RType> RType make(RType obj, ThrowingConsumer<RType> consumer) {
        onLeave(() -> consumer.apply(obj));
        return obj;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        ScopeException toThrow = null;
        for (int position = leaveActions.size() - 1; position >= 0; --position) {
            try {
                leaveActions.get(position).perform();
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
        return leaveActions.size();
    }

}
