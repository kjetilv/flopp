package com.github.kjetilv.lopp.utils;

public final class Non {

    private Non() {
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

    public static int negative(int i, String name) {
        if (i >= 0) {
            return i;
        }
        throw new IllegalStateException("Expected non-negative " + name + ": " + i);
    }

    public static short negative(short i, String name) {
        if (i >= 0) {
            return i;
        }
        throw new IllegalStateException("Expected non-negative " + name + ": " + i);
    }

    public static long negative(long l, String name) {
        if (l >= 0) {
            return l;
        }
        throw new IllegalStateException("Expected non-negative " + name + ": " + l);
    }
}
