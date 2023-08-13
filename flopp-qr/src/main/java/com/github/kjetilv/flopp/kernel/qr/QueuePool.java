package com.github.kjetilv.flopp.kernel.qr;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class QueuePool<T> {

    private final Queue<T> queue;

    private final Supplier<T> create;

    private final Consumer<T> release;

    QueuePool(Supplier<T> create, Consumer<T> release) {
        this.create = Objects.requireNonNull(create, "create");
        this.release = release;
        this.queue = new LinkedTransferQueue<>();
    }

    T acquire() {
        return Optional.ofNullable(queue.poll()).orElseGet(create);
    }

    void release(T t) {
        release.accept(t);
        this.queue.offer(t);
    }
}
