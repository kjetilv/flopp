package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

final class Futures {

    static <T> List<T> awaitCompleted(Stream<CompletableFuture<T>> futures) {
        return futures
            .toList()
            .stream()
            .map(CompletableFuture::join)
            .toList();
    }

    private Futures() {
    }
}
