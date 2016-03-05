package com.asprotunity.queryiteasy.disposer;

import java.util.ArrayList;


public class Disposer implements AutoCloseable {

    private boolean isClosed = false;
    private ArrayList<CloseHandler> handlers = new ArrayList<>();

    public void onClose(CloseHandler handler) {
        this.handlers.add(handler);
    }

    @Override
    public void close() {
        DisposerException toThrow = null;
        for (int position = handlers.size() - 1; position >= 0; --position) {
            try {
                CloseHandler handler = handlers.get(position);
                handler.apply();
            } catch (Exception exception) {
                if (toThrow == null) {
                    toThrow = new DisposerException(exception);
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
        return handlers.size();
    }

}
