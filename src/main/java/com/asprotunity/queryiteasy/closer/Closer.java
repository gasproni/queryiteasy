package com.asprotunity.queryiteasy.closer;

import java.util.ArrayList;


public class Closer implements AutoCloseable {

    private boolean isClosed = false;
    private ArrayList<CloseAction> closeActions = new ArrayList<>();

    public void onClose(CloseAction caller) {
        this.closeActions.add(caller);
    }

    @Override
    public void close() {
        CloserException toThrow = null;
        for (int position = closeActions.size() - 1; position >= 0; --position) {
            try {
                CloseAction closeAction = closeActions.get(position);
                closeAction.perform();
            } catch (Exception exception) {
                if (toThrow == null) {
                    toThrow = new CloserException(exception);
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
        return closeActions.size();
    }

}
