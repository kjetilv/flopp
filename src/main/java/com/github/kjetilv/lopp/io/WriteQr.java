package com.github.kjetilv.lopp.io;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface WriteQr<T> extends Consumer<T>, Runnable, Closeable {

    static <T> WriteQr<T> create(Consumer<T> sink, int queueLength, T poison) {
        return create(null, sink, queueLength, poison);
    }

    static <T> WriteQr<T> create(String name, Consumer<T> sink, int queueLength, T poison) {
        return new AsynchWriteQr<>(name, sink, queueLength, poison);
    }

    static WriteQr<String> create(Consumer<String> sink, int queueLength) {
        return create(null, sink, queueLength);
    }

    static WriteQr<String> create(String name, Consumer<String> sink, int queueLength) {
        return new AsynchWriteQr<>(name, sink, queueLength, UUID.randomUUID().toString());
    }

    default WriteQr<T> in(ExecutorService executorService) {
        executorService.submit(this);
        return this;
    }

    void awaitAllProcessed();

    @Override
    void close();
}
