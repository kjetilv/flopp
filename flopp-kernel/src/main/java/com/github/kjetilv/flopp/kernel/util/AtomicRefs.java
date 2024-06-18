package com.github.kjetilv.flopp.kernel.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class AtomicRefs<T> {

    private final Lock[] locks;

    private final Object[] array;

    public AtomicRefs(int size) {
        this.array = new Object[size];
        this.locks = new Lock[size];
        for (int i = 0; i < array.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @SuppressWarnings("unchecked")
    public T computeIfAbsent(int i, Supplier<T> ifAbsent) {
        Object existing = array[i];
        if (existing == null) {
            locks[i].lock();
            try {
                Object obj = array[i];
                return (T) (obj == null
                    ? array[i] = ifAbsent.get()
                    : obj);
            } finally {
                locks[i].unlock();
            }
        }
        return (T) existing;
    }
}
