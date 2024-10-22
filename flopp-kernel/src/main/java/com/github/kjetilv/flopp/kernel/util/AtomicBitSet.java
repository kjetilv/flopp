package com.github.kjetilv.flopp.kernel.util;

import java.util.concurrent.atomic.AtomicIntegerArray;

@SuppressWarnings("unused")
public final class AtomicBitSet {

    private final AtomicIntegerArray ints;

    public AtomicBitSet(int length) {
        this.ints = new AtomicIntegerArray(Non.negativeOrZero(length, "length") + 31 >>> 5);
    }

    public boolean get(long n) {
        int bit = bit(n);
        int index = index(n);
        int current = ints.get(index);
        return (current & bit) != 0;
    }

    public boolean set(long n) {
        int bit = bit(n);
        int index = index(n);
        do {
            int current = ints.get(index);
            int updated = current | bit;
            if (current == updated) {
                return false;
            }
            if (ints.compareAndSet(index, current, updated)) {
                return true;
            }
        } while (true);
    }

    private static int index(long n) {
        return (int) (n >>> 5);
    }

    private static int bit(long n) {
        return 1 << n;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[ints:" + ints.length() + "]";
    }
}