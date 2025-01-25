package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

final class ResultConsumerSpliterator<T> extends Spliterators.AbstractSpliterator<PartitionResult<T>>
    implements Consumer<PartitionResult<T>> {

    private final int resultsCount;

    private final AtomicInteger lastServed = new AtomicInteger(-1);

    private final Map<Integer, PartitionResult<T>> completed;

    private final ToLongFunction<T> sizer;

    private final Lock updatedLock = new ReentrantLock();

    private final Condition updatedCondition = updatedLock.newCondition();

    private final Map<T, Long> cachedSizes = new ConcurrentHashMap<>();

    ResultConsumerSpliterator(int resultsCount, ToLongFunction<T> sizer) {
        super(resultsCount, SIZED | IMMUTABLE | ORDERED);
        this.resultsCount = resultsCount;
        this.completed = new HashMap<>(resultsCount);
        this.sizer = sizer;
    }

    @Override
    public void accept(PartitionResult<T> partitionResult) {
        Objects.requireNonNull(partitionResult, "future");
        updatedLock.lock();
        try {
            PartitionResult<T> duplicate =
                completed.putIfAbsent(partitionResult.partition().partitionNo(), partitionResult);
            if (duplicate != null) {
                throw new IllegalStateException(
                    "Partition " + partitionResult.partition().partitionNo() + " already present: " + duplicate);
            }
            updatedCondition.signalAll();
        } finally {
            updatedLock.unlock();
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super PartitionResult<T>> action) {
        if (done()) {
            return false;
        }
        updatedLock.lock();
        try {
            PartitionResult<T> next = completed.get(nextIndex());
            if (next == null) {
                try {
                    updatedCondition.await();
                    return true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted", e);
                }
            }
            Partition adjustedPartition = next.partition().at(
                sumOfPreceding(next),
                size(next.result())
            );
            PartitionResult<T> result = next.withAdjustedPartition(adjustedPartition);
            action.accept(result);
            lastServed.incrementAndGet();
            return !done();
        } finally {
            updatedLock.unlock();
        }
    }

    private int nextIndex() {
        return lastServed.get() + 1;
    }

    private boolean done() {
        return lastServed.get() == resultsCount - 1;
    }

    private long sumOfPreceding(PartitionResult<T> partitionResult) {
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + cachedSizes + ", completed: " + completed.keySet() + "]";
    }
}
