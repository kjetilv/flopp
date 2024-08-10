package com.github.kjetilv.flopp.kernel.util;

import java.util.Collection;

@SuppressWarnings({"unused", "UnusedReturnValue"})
public final class Non {

    public static <T, C extends Collection<T>> C empty(C cs, String contents) {
        if (cs == null || cs.isEmpty()) {
            throw new IllegalArgumentException("Empty collection: " + contents);
        }
        return cs;
    }

    public static int negativeOrZero(int i, String name) {
        if (i > 0) {
            return i;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + i);
    }

    public static short negativeOrZero(short i, String name) {
        if (i > 0) {
            return i;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + i);
    }

    public static long negativeOrZero(long l, String name) {
        if (l > 0) {
            return l;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + l);
    }

    public static double negativeOrZero(double d, String name) {
        if (d > 0) {
            return d;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + d);
    }

    public static int negative(int i, String name) {
        if (i < 0) {
            throw new IllegalStateException("Expected non-negative " + name + ": " + i);
        }
        return i;
    }

    public static short negative(short i, String name) {
        if (i < 0) {
            throw new IllegalStateException("Expected non-negative " + name + ": " + i);
        }
        return i;
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
