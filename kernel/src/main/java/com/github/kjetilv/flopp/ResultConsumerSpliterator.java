package com.github.kjetilv.flopp;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

class ResultConsumerSpliterator<T> extends Spliterators.AbstractSpliterator<PartitionResult<T>>
    implements Consumer<CompletableFuture<PartitionResult<T>>> {

    private final int resultsCount;

    private final AtomicInteger lastServed = new AtomicInteger(-1);

    private final Map<Integer, PartitionResult<T>> completed;

    private final ToLongFunction<T> sizer;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private final Lock updateLock = new ReentrantLock();

    private final Condition updated = updateLock.newCondition();

    private final Map<T, Long> cachedSizes = new ConcurrentHashMap<>();

    ResultConsumerSpliterator(int resultsCount, ToLongFunction<T> sizer) {
        super(resultsCount, SIZED | IMMUTABLE | ORDERED);
        this.resultsCount = resultsCount;
        this.completed = new HashMap<>(resultsCount);
        this.sizer = sizer;
    }

    @Override
    public void accept(CompletableFuture<PartitionResult<T>> future) {
        future.whenComplete(this::recordCompletion);
    }

    @Override
    public boolean tryAdvance(Consumer<? super PartitionResult<T>> action) {
        if (done()) {
            return false;
        }
        updateLock.lock();
        try {
            PartitionResult<T> nextPartitionResult = completed.get(nextIndex());
            if (nextPartitionResult == null) {
                awaitNextUpdate();
                return true;
            }
            action.accept(nextPartitionResult.withAdjustedPartition(
                sizeOfPrevious(nextPartitionResult),
                size(nextPartitionResult.result())
            ));
            lastServed.incrementAndGet();
            return !done();
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + cachedSizes + ", completed:" + completed.keySet() + "]";
    }

    protected void recordCompletion(PartitionResult<T> future, Throwable e) {
        Objects.requireNonNull(future, "future");
        updateLock.lock();
        try {
            completed.put(future.partition().partitionNo(), future);
            updated.signalAll();
        } finally {
            updateLock.unlock();
        }
        if (e != null) {
            failure.updateAndGet(existing -> existing == null ? e : existing);
        }
    }

    private int nextIndex() {
        return lastServed.get() + 1;
    }

    private boolean done() {
        return lastServed.get() == resultsCount - 1;
    }

    private void awaitNextUpdate() {
        try {
            updated.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }

    private long sizeOfPrevious(PartitionResult<T> partitionResult) {
        return completed.values()
            .stream()
            .filter(completedResult ->
                completedResult.partition().partitionNo() < partitionResult.partition().partitionNo())
            .mapToLong(completedResult ->
                size(completedResult.result()))
            .sum();
    }

    private long size(T path) {
        return cachedSizes.computeIfAbsent(path, sizer::applyAsLong);
    }
}
