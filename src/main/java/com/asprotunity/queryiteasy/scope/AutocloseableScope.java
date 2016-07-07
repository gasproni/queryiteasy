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
    public void close() {
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
