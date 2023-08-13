package com.github.kjetilv.lopp.utils;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class QueuePool<T> implements Pool<T> {

    private final Queue<T> queue;

    private final Supplier<T> create;

    private final Consumer<T> release;

    public QueuePool(Supplier<T> create, Consumer<T> release) {
        this(0, create, release);
    }

    public QueuePool(Supplier<T> create) {
        this(0, create);
    }

    public QueuePool(int size, Supplier<T> create) {
        this(size, create, null);
    }

    public QueuePool(int size, Supplier<T> create, Consumer<T> release) {
        this.create = Objects.requireNonNull(create, "create");
        this.release = release;
        this.queue = size > 0 ? new ArrayBlockingQueue<>(size) : new LinkedTransferQueue<>();
    }

    @Override
    public T acquire() {
        return Optional.ofNullable(queue.poll()).orElseGet(create);
    }

    @Override
    public void release(T t) {
        if (release != null) {
            release.accept(t);
        }
        this.queue.offer(t);
    }
}
