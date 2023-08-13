package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.FileShape;
import com.github.kjetilv.lopp.Partitioning;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class AsyncLineCounter implements LineCounter {

    private final ExecutorService executorService;

    private final FileShape fileShape;

    private final Partitioning partitioning;

    public AsyncLineCounter(FileShape fileShape, Partitioning partitioning, ExecutorService executorService) {
        this.fileShape = Objects.requireNonNull(fileShape, "fileShape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public Lines scan(Path path) {
        try (
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                path,
                Set.of(StandardOpenOption.READ),
                executorService
            )
        ) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(partitioning.bufferSize());
            Lock completeLock = new ReentrantLock();
            Condition completed = completeLock.newCondition();
            LineCountingCompletionHandler handler =
                new LineCountingCompletionHandler(
                    fileShape,
                    partitioning,
                    channel,
                    byteBuffer,
                    0L,
                    partitioning.bufferSize(),
                    fileShape.fileSize(),
                    () -> {
                        completeLock.lock();
                        try {
                            completed.signalAll();
                        } finally {
                            completeLock.unlock();
                        }
                    }
                );
            List<ByteIndexEstimator> estimators = Collections.synchronizedList(new ArrayList<>());
            channel.read(
                byteBuffer,
                0,
                estimators,
                handler
            );
            awaitSignalDone(completeLock, completed);
            return combined(estimators);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    private static ByteIndexEstimator combined(List<ByteIndexEstimator> estimators) {
        return estimators.stream().reduce(ByteIndexEstimator::combine).orElseThrow();
    }

    private static void awaitSignalDone(Lock completeLock, Condition completed) {
        completeLock.lock();
        try {
            completed.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } finally {
            completeLock.unlock();
        }
    }
}
