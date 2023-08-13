package com.github.kjetilv.flopp.kernel.qr;

public final class Non {

    public static int negativeOrZero(int i, String name) {
        if (i > 0) {
            return i;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + i);
    }

    public static long negative(long l, String name) {
        if (l < 0) {
            throw new IllegalStateException("Expected non-negative " + name + ": " + l);
        }
        return l;
    }

    private Non() {
    }
}
