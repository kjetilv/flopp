package com.github.kjetilv.flopp.kernel.util;

import java.util.concurrent.atomic.AtomicIntegerArray;

public final class AtomicBitSet {

    private final AtomicIntegerArray array;

    public AtomicBitSet(int length) {
        this.array = new AtomicIntegerArray(length + 31 >>> 5);
    }

    public boolean set(long n) {
        int bit = bit(n);
        int idx = index(n);
        do {
            int current = get(idx);
            int updated = current | bit;
            if (current == updated) {
                return false;
            }
            if (array.compareAndSet(idx, current, updated)) {
                return true;
            }
        } while (true);
    }

    public boolean get(long n) {
        int bit = bit(n);
        int index = index(n);
        int existingValue = get(index);
        return (existingValue & bit) != 0;
    }

    private int get(int idx) {
        return array.get(idx);
    }

    private static int index(long n) {
        return (int) (n >>> 5);
    }

    private static int bit(long n) {
        return 1 << n;
    }
}