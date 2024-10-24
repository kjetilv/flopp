package com.github.kjetilv.flopp.kernel.util;

import java.util.concurrent.atomic.AtomicIntegerArray;

@SuppressWarnings("unused")
public final class AtomicBitSet {

    private final AtomicIntegerArray ints;

    public AtomicBitSet(int length) {
        int len = Non.negativeOrZero(length, "length");
        this.ints = new AtomicIntegerArray(len + 31 >>> 5);
    }

    public boolean get(long n) {
        int current = get(index(n));
        return (current & bit(n)) != 0;
    }

    public boolean set(long n) {
        int bit = bit(n);
        int index = index(n);
        do {
            int current = get(index);
            int updated = current | bit;
            if (current == updated) {
                return false;
            }
            if (wasSet(index, current, updated)) {
                return true;
            }
        } while (true);
    }

    private int get(int index) {
        return ints.get(index);
    }

    private boolean wasSet(int index, int current, int updated) {
        return ints.compareAndSet(index, current, updated);
    }

    private static int index(long n) {
        return (int) (n >>> 5);
    }

    private static int bit(long n) {
        return 1 << n;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + ints.length() * 4 + " bits]";
    }
}