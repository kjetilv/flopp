package com.github.kjetilv.lopp.io;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@FunctionalInterface
public interface ReadQr {

    static ReadQr create(ExecutorService executorService) {
        return new AsyncReadQr(executorService);
    }

    default Stream<String> streamStrings(Stream<String> ioStream, int queueSize) {
        return streamStrings(ioStream, 0L, queueSize);
    }

    default Stream<String> streamStrings(Stream<String> ioStream, long count, int queueSize) {
        return stream(ioStream, count, queueSize, UUID.randomUUID().toString());
    }

    default <T> Stream<T> stream(Stream<T> ioStream, int queueSize, T poisonPill) {
        return stream(ioStream, 0, queueSize, poisonPill);
    }

    default <T> Stream<T> stream(String name, Stream<T> ioStream, int queueSize, T poisonPill) {
        return stream(name, ioStream, 0, queueSize, poisonPill);
    }

    default<T> Stream<T> stream(Stream<T> ioStream, long count, int queueSize, T poisonPill) {
        return stream(null, ioStream, count, queueSize, poisonPill);
    }

    <T> Stream<T> stream(String name, Stream<T> ioStream, long count, int queueSize, T poisonPill);
}
