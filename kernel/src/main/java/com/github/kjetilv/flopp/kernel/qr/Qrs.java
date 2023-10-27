package com.github.kjetilv.flopp.kernel.qr;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public final class Qrs {

    public static ReadQr reader(ExecutorService executorService) {
        return new AsyncReadQr(executorService, QUEUE_POLL);
    }

    public static <T> WriteQr<T> writer(Consumer<T> sink, int queueLength, T poison) {
        return writer(null, sink, queueLength, poison);
    }

    public static <T> WriteQr<T> writer(String name, Consumer<T> sink, int queueLength, T poison) {
        return new AsynchWriteQr<>(name, sink, queueLength, QUEUE_POLL, poison);
    }

    public static WriteQr<String> writer(Consumer<String> sink, int queueLength) {
        return writer(null, sink, queueLength);
    }

    public static WriteQr<String> writer(String name, Consumer<String> sink, int queueLength) {
        return new AsynchWriteQr<>(
            name,
            sink,
            queueLength,
            QUEUE_POLL,
            UUID.randomUUID().toString()
        );
    }

    private Qrs() {

    }

    private static final Duration QUEUE_POLL = Duration.ofSeconds(1);
}
