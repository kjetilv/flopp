package com.github.kjetilv.lopp.io;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

final class MultiVial<T> implements Vial<T> {

    private final List<T> ts;

    private final boolean done;

    private final int size;

    private final Runnable onCompletion;

    private final AtomicBoolean completed = new AtomicBoolean();

    MultiVial(boolean done) {
        this(null, done, null);
    }

    MultiVial(List<T> ts, boolean done, Runnable onCompletion) {
        this.ts = ts == null ? Collections.emptyList() : ts;
        this.done = done;
        int size = this.ts.size();
        this.size = size == 0 ? 0 : this.done ? size - 1 : size;
        this.onCompletion = onCompletion == null ? () -> {
        } : onCompletion;
    }

    @Override
    public void close() {
        if (completed.compareAndSet(false, true)) {
            onCompletion.run();
        }
    }

    /**
     * @return What we tapped.
     */
    @Override public Stream<T> contents() {
        if (done) {
            return ts.stream().limit(size);
        }
        return ts.stream();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean hasResults() {
        return size > 0;
    }

    @Override
    public boolean done() {
        return done;
    }
}
