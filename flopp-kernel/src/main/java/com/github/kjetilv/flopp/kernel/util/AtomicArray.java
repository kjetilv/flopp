package com.github.kjetilv.flopp.kernel.util;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class AtomicArray<T> {

    private final Lock[] locks;

    private final Object[] array;

    public AtomicArray(int size) {
        this.array = new Object[size];
        this.locks = new Lock[size];
        for (int i = 0; i < array.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @SuppressWarnings("unchecked")
    public T computeIfAbsent(int i, Supplier<T> ifAbsent) {
        locks[i].lock();
        try {
            Object obj = array[i];
            return (T) (obj == null
                ? array[i] = get(ifAbsent)
                : obj);
        } finally {
            locks[i].unlock();
        }
    }

    private static <T> T get(Supplier<T> ifAbsent) {
        return Objects.requireNonNull(ifAbsent.get(), () -> "Non-null return: " + ifAbsent);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + locks.length + "]";
    }
}
