package com.github.kjetilv.flopp.kernel.qr;

import java.time.Duration;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class AsyncReadQr implements ReadQr {

    private final ExecutorService executor;

    private final Duration queuePoll;

    AsyncReadQr(ExecutorService executor, Duration queuePoll) {
        this.executor = Objects.requireNonNull(executor, "this.pool");
        this.queuePoll = Objects.requireNonNull(queuePoll, "queuePoll");
    }

    @Override
    public <T> Stream<T> stream(String name, Stream<T> stream, long count, int queueSize, T poisonPill) {
        Vein<T> vein = new Bloodstream<>(
            name,
            Non.negativeOrZero(queueSize, "queueSize"),
            queuePoll,
            Objects.requireNonNull(poisonPill, "poisonPill")
        );

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Function<Throwable, Void> failureHandler = e -> {
            failure.set(e);
            return null;
        };

        CompletableFuture.runAsync(
            new Injector<>(vein, stream, failure), executor).exceptionallyAsync(failureHandler);
        return StreamSupport.stream(
            new Tapper<>(vein, Non.negative(count, "count"), failure),
            false
        ).onClose(stream::close);
    }

    private static final class Injector<T> implements Runnable {

        private final Vein<T> vein;

        private final Stream<T> stream;

        private final AtomicReference<Throwable> failure;

        private Injector(Vein<T> vein, Stream<T> stream, AtomicReference<Throwable> failure) {
            this.vein = Objects.requireNonNull(vein, "queuer");
            this.stream = Objects.requireNonNull(stream, "stream");
            this.failure = failure;
        }

        @Override
        public void run() {
            try {
                stream.forEach(vein::inject);
            } catch (Exception e) {
                failure.set(e);
            } finally {
                vein.close();
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[[" + vein + "] <- " + stream + "]";
        }
    }

    private static final class Tapper<T> extends Spliterators.AbstractSpliterator<T> {

        private final Vein<T> vein;

        private final AtomicReference<Throwable> failure;

        private Tapper(
            Vein<T> vein,
            long count,
            AtomicReference<Throwable> failure
        ) {
            super(count > 0 ? count : Long.MAX_VALUE, IMMUTABLE);
            this.vein = Objects.requireNonNull(vein, "queuer");
            this.failure = failure;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> consumer) {
            Throwable failure = this.failure.get();
            if (failure != null) {
                throw new IllegalStateException("Stream failed in reading thread: " + failure);
            }
            try (Vial<T> vial = vein.tap()) {
                vial.contents().forEach(consumer);
                return !vial.done();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to tap " + vein, e);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + vein + "]";
        }
    }
}
