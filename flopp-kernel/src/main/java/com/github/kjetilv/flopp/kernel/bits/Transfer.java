package com.github.kjetilv.flopp.kernel.bits;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

@FunctionalInterface
public interface Transfer extends Runnable, Closeable, Supplier<Void> {

    default CompletableFuture<Void> in(ExecutorService executorService) {
        return CompletableFuture.runAsync(this, executorService);
    }

    @Override
    default Void get() {
        this.run();
        return null;
    }

    @Override
    default void close() {
    }
}
