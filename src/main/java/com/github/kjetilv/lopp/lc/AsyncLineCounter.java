package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.Shape;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("unused")
public final class AsyncLineCounter {

    private final ExecutorService executorService;

    private final int bufferSize;

    public AsyncLineCounter(ExecutorService executorService) {
        this(executorService, 0);
    }

    public AsyncLineCounter(ExecutorService executorService, int bufferSize) {
        this.executorService = Objects.requireNonNull(executorService, "executorService");
        this.bufferSize = bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE;
    }

    public long count(Path path, Shape shape) {
        try (
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                path,
                Set.of(StandardOpenOption.READ),
                executorService
            )
        ) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
            Lock completeLock = new ReentrantLock();
            Condition completed = completeLock.newCondition();
            LongAdder linesCount = new LongAdder();
            long size = shape.size();
            LineCountingCompletionHandler handler =
                new LineCountingCompletionHandler(
                    channel,
                    byteBuffer,
                    0L,
                    bufferSize,
                    size,
                    () -> {
                        completeLock.lock();
                        try {
                            completed.signalAll();
                        } finally {
                            completeLock.unlock();
                        }
                    }
                );
            channel.read(byteBuffer, 0, linesCount, handler);
            awaitSignalDone(completeLock, completed);
            return linesCount.longValue();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;

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
