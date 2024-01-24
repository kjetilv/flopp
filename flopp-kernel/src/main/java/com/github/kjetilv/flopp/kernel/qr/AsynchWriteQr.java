package com.github.kjetilv.flopp.kernel.qr;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

final class AsynchWriteQr<T> implements WriteQr<T> {

    private final Vein<T> vein;

    private final Consumer<T> sink;

    AsynchWriteQr(String name, Consumer<T> sink, int queueLength, Duration queuePoll, T poison) {
        this(
            new Bloodstream<>(name, queueLength, queuePoll, poison),
            Objects.requireNonNull(sink, "io")
        );
    }

    private AsynchWriteQr(Vein<T> vein, Consumer<T> sink) {
        this.vein = vein;
        this.sink = sink;
    }

    @Override
    public WriteQr<T> in(ExecutorService executorService) {
        Objects.requireNonNull(executorService, "executorService").submit(this);
        return this;
    }

    public void awaitAllProcessed() {
        vein.drain();
    }

    public void close() {
        vein.close();
        vein.drain();
    }

    public void run() {
        try {
            while (true) {
                try (Vial<T> vial = vein.tap()) {
                    vial.contents()
                        .forEach(sink);
                    if (vial.done()) {
                        return;
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unknown failure: " + this, e);
        }
    }

    @Override
    public void accept(T t) {
        vein.inject(Objects.requireNonNull(t, "t"));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + vein + "]";
    }
}
