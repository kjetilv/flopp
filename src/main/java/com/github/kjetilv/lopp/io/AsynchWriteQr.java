package com.github.kjetilv.lopp.io;

import com.github.kjetilv.lopp.utils.Non;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class AsynchWriteQr<T> implements WriteQr<T> {

    private final Vein<T> vein;

    private final Consumer<T> sink;

    AsynchWriteQr(String name, Consumer<T> sink, int queueLength, T poison) {
        this.vein = new MultiVein<>(
            name,
            Non.negativeOrZero(queueLength, "queueLength"),
            Objects.requireNonNull(poison, "poison")
        );
        this.sink = Objects.requireNonNull(sink, "io");
    }

    public void run() {
        try {
            while (true) {
                try (Vial<T> vial = vein.tap()) {
                    if (vial.hasResults()) {
                        vial.contents().forEach(sink);
                    }
                    if (vial.done()) {
                        return;
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unknown failure: " + this, e);
        }
    }

    public void awaitAllProcessed() {
        vein.drain();
    }

    public void close() {
        vein.close();
        vein.drain();
    }

    @Override
    public void accept(T t) {
        acceptSingle(t);
    }

    private void acceptSingle(T t) {
        vein.inject(Objects.requireNonNull(t, "t"));
    }

    private void acceptMulti(List<T> ts) {
        vein.inject(Objects.requireNonNull(ts, "ts"));
    }

    private void process(Stream<T> ts) {
        ts.forEach(sink);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + vein + "]";
    }
}
