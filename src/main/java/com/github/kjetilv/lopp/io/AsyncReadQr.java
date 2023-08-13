package com.github.kjetilv.lopp.io;

import com.github.kjetilv.lopp.utils.Non;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class AsyncReadQr implements ReadQr {

    private static final Logger log = LoggerFactory.getLogger(AsyncReadQr.class);

    private final ExecutorService executor;

    AsyncReadQr(ExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "this.pool");
    }

    @Override
    public <T> Stream<T> stream(String name, Stream<T> stream, long count, int queueSize, T poisonPill) {
        Vein<T> vein = new MultiVein<>(
            name,
            Non.negativeOrZero(queueSize, "queueSize"),
            Objects.requireNonNull(poisonPill, "poisonPill")
        );
        AtomicReference<Exception> failure = new AtomicReference<>();
        CompletableFuture.runAsync(
            new Injector<T>(vein, stream, failure),
            executor
        ).whenComplete((__, e) -> {
            if (e != null) {
                log.error("Async read failed: {}", stream, e);
            }
        });
        return StreamSupport.stream(
            new Tapper<T>(vein, Non.negative(count, "count"), failure),
            false
        ).onClose(stream::close);
    }

    private static final class Injector<T> implements Runnable {

        private final Vein<T> vein;

        private final Stream<T> stream;

        private final AtomicReference<Exception> failure;

        private Injector(Vein<T> vein, Stream<T> stream, AtomicReference<Exception> failure) {
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
                log.error("{}: Streaming failed", this, e);
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

        private final AtomicReference<Exception> failure;

        private Tapper(Vein<T> vein, long count, AtomicReference<Exception> failure) {
            super(count > 0 ? count : Long.MAX_VALUE, IMMUTABLE);
            this.vein = Objects.requireNonNull(vein, "queuer");
            this.failure = failure;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> consumer) {
            Exception exception = failure.get();
            if (exception != null) {
                throw new IllegalStateException("Stream failed in reading thread: " + exception);
            }
            try (Vial<T> vial = vein.tap()) {
                if (vial.hasResults()) {
                    vial.contents().forEach(consumer);
                }
                return !vial.done();
            } catch (Exception e) {
                throw new IllegalStateException("Unknown failure", e);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + vein + "]";
        }
    }
}
