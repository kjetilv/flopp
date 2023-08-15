package com.github.kjetilv.lopp;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

final class Futures {

    private Futures() {

    }

    static <T> List<T> await(Stream<CompletableFuture<T>> futures) {
        return await(futures.toList());
    }

    private static <T> List<T> await(List<CompletableFuture<T>> futures) {
        return futures.stream()
            .map(CompletableFuture::join)
            .toList();
    }
}
