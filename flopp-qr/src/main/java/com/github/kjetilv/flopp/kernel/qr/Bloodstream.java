package com.github.kjetilv.flopp.kernel.qr;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class Bloodstream<T> implements Vein<T> {

    private final String name;

    private final BlockingQueue<T> ts;

    private final int capacity;

    private final Duration queuePoll;

    private final T poison;

    private final AtomicBoolean poisoned = new AtomicBoolean();

    private final AtomicBoolean dead = new AtomicBoolean();

    private final Lock processLock = new ReentrantLock();

    private final Condition queueEmptied = processLock.newCondition();

    private final LongAdder unprocessed = new LongAdder();

    private final QueuePool<List<T>> pool;

    Bloodstream(String name, int length, Duration queuePoll, T poison) {
        this.name = name;
        this.capacity = Non.negativeOrZero(length, "length");
        this.queuePoll = Objects.requireNonNull(queuePoll, "queuePoll");
        this.ts = new LinkedBlockingQueue<>(this.capacity);
        this.poison = Objects.requireNonNull(poison, "poison");
        this.pool = new QueuePool<>(() -> new ArrayList<>(length), List::clear);
    }

    @Override
    public void inject(T t) {
        addAudited(t);
    }

    @Override
    public void inject(Collection<T> ts) {
        if (ts != null) {
            for (T t : Objects.requireNonNull(ts, "ts")) {
                addAudited(t);
            }
        }
    }

    @Override
    public Vial<T> tap() {
        if (dead.get()) {
            return new MultiVial<>(true);
        }
        try {
            T t = ts.poll(queuePoll.toMillis(), MILLISECONDS);
            if (t == null) {
                return new MultiVial<>(poisoned.get());
            }
            List<T> tapped = pool.acquire();
            tapped.add(t);
            ts.drainTo(tapped, capacity - 1);
            boolean tappedPoison = tapped.contains(poison);
            if (tappedPoison) {
                dead.compareAndSet(false, true);
            }
            return new MultiVial<>(
                tapped,
                tappedPoison,
                () -> {
                    try {
                        wasProcessed(tapped);
                    } finally {
                        pool.release(tapped);
                    }
                }
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }

    @Override
    public void drain() {
        processLock.lock();
        try {
            while (true) {
                if (this.unprocessed.longValue() > 0) {
                    await(this.queueEmptied);
                } else {
                    return;
                }
            }
        } finally {
            processLock.unlock();
        }
    }

    @Override
    public void close() {
        if (poisoned.compareAndSet(false, true)) {
            add(poison);
        }
    }

    private void addAudited(T t) {
        if (poisoned.get()) {
            throw new IllegalArgumentException("Closed");
        }
        try {
            add(t);
        } finally {
            this.unprocessed.increment();
        }
    }

    private void add(T t) {
        try {
            ts.put(t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(this + " interrupted", e);
        }
    }

    private void wasProcessed(List<T> ts) {
        processLock.lock();
        try {
            try {
                this.unprocessed.add(-ts.size());
            } finally {
                if (this.unprocessed.longValue() <= 0) {
                    this.queueEmptied.signalAll();
                }
            }
        } finally {
            processLock.unlock();
        }
    }

    private static void await(Condition condition) {
        try {
            condition.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" +
               (name == null ? hashCode() : name) + ":" + capacity + (poisoned.get() ? ", shutdown" : "") +
               "]";
    }
}
