package com.github.kjetilv.flopp.kernel.util;

public final class Todo {

    public static <T> T TODO() {
        throw new UnsupportedOperationException("TODO");
    }

    public static <T> T TODO(String message) {
        throw new UnsupportedOperationException("TODO: " + message);
    }

    private Todo() {
    }
}
