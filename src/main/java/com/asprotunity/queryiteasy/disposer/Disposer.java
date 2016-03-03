package com.asprotunity.queryiteasy.disposer;

import java.util.ArrayList;
import java.util.Stack;

public interface Disposer extends AutoCloseable {
    void onClose(CloseHandler closeHandler);

    @Override
    void close();

    boolean isClosed();

    int handlersCount();

    static Disposer makeNew() {
        return new Disposer() {
            private ArrayList<CloseHandler> handlers = new ArrayList<>();
            private boolean isClosed = false;

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

            @Override
            public boolean isClosed() {
                return isClosed;
            }

            @Override
            public int handlersCount() {
                return handlers.size();
            }

            public void onClose(CloseHandler handler) {
                this.handlers.add(handler);
            }
        };
    }
}
