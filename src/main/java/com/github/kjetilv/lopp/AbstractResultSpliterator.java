package com.github.kjetilv.lopp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

abstract class AbstractResultSpliterator
    extends Spliterators.AbstractSpliterator<PartitionedFile.Result<Path>> {

    private final int resultsCount;

    private final AtomicInteger lastServed = new AtomicInteger(-1);

    private final Map<Integer, PartitionedFile.Result<Path>> completed;

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private final Lock updateLock = new ReentrantLock();

    private final Condition updated = updateLock.newCondition();

    private final Map<Path, Long> fileSizes = new ConcurrentHashMap<>();

    AbstractResultSpliterator(int resultsCount) {
        super(resultsCount, SIZED | IMMUTABLE | ORDERED);
        this.resultsCount = resultsCount;
        this.completed =
            new ConcurrentHashMap<>(resultsCount * 2, 0.75f, resultsCount);
    }

    @Override
    public boolean tryAdvance(Consumer<? super PartitionedFile.Result<Path>> action) {
        updateLock.lock();
        try {
            while (true) {
                if (lastServed.get() == resultsCount - 1) {
                    return false;
                }
                int nextIndex = lastServed.get() + 1;
                PartitionedFile.Result<Path> nextResult = completed.get(nextIndex);
                if (nextResult != null) {
                    action.accept(sourcePartitioned(nextResult));
                    lastServed.incrementAndGet();
                } else {
                    try {
                        updated.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted", e);
                    }
                }
            }
        } finally {
            updateLock.unlock();
        }
    }

    protected void recordCompletion(PartitionedFile.Result<Path> future, Throwable e) {
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

    private PartitionedFile.Result<Path> sourcePartitioned(PartitionedFile.Result<Path> result) {
        return result.asPartition(sizeOfPrevious(result), sizeOf(result.result()));
    }

    private long sizeOfPrevious(PartitionedFile.Result<Path> result) {
        return completed.values()
            .stream()
            .filter(completedResult ->
                completedResult.partition().partitionNo() < result.partition().partitionNo())
            .mapToLong(completedResult ->
                sizeOf(completedResult.result()))
            .sum();
    }

    private long sizeOf(Path path) {
        return fileSizes.computeIfAbsent(path, __ -> {
            try {
                return Files.size(path);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to size " + path, e);
            }
        });
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + fileSizes + ", completed:" + completed.keySet() + "]";
    }
}
