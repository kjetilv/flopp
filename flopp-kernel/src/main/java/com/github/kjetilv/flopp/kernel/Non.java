package com.github.kjetilv.flopp.kernel;

@SuppressWarnings("unused")
public final class Non {

    private Non() {
    }

    public static int negativeOrZero(int i, String name) {
        if (pos(i)) {
            return i;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + i);
    }

    public static short negativeOrZero(short i, String name) {
        if (pos(i)) {
            return i;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + i);
    }

    public static long negativeOrZero(long l, String name) {
        if (pos(l)) {
            return l;
        }
        throw new IllegalStateException("Expected non-zero " + name + ": " + l);
    }

    public static int negative(int i, String name) {
        if (neg(i)) {
            throw new IllegalStateException("Expected non-negative " + name + ": " + i);
        }
        return i;
    }

    public static short negative(short i, String name) {
        if (neg(i)) {
            throw new IllegalStateException("Expected non-negative " + name + ": " + i);
        }
        return i;
    }

    public static long negative(long l, String name) {
        if (neg(l)) {
            throw new IllegalStateException("Expected non-negative " + name + ": " + l);
        }
        return l;
    }

    private static boolean pos(long l) {
        return l > 0;
    }

    private static boolean neg(long l) {
        return l < 0;
    }
}
