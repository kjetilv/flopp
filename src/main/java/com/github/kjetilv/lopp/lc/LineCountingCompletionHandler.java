package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.FileShape;
import com.github.kjetilv.lopp.Partitioning;
import com.github.kjetilv.lopp.utils.Non;
import com.github.kjetilv.lopp.utils.Pool;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

final class LineCountingCompletionHandler implements CompletionHandler<Integer, List<ByteIndexEstimator>> {

    private final FileShape fileShape;

    private final Partitioning partitioning;

    private final AsynchronousFileChannel channel;

    private final ByteBuffer byteBuffer;

    private final long offset;

    private final int length;

    private final long total;

    private final Runnable signalDone;

    private final Pool<ByteBuffer> byteBuffers;

    private final Pool<byte[]> buffers;

    private final AtomicInteger reads;

    LineCountingCompletionHandler(
        FileShape fileShape,
        Partitioning partitioning,
        AsynchronousFileChannel channel,
        ByteBuffer byteBuffer,
        long offset,
        int length,
        long total,
        Runnable signalDone
    ) {
        this(
            fileShape,
            partitioning,
            channel,
            byteBuffer,
            offset,
            length,
            total,
            signalDone,
            null,
            null,
            null
        );
    }

    private LineCountingCompletionHandler(
        FileShape fileShape,
        Partitioning partitioning,
        AsynchronousFileChannel channel,
        ByteBuffer byteBuffer,
        long offset,
        int length,
        long total,
        Runnable signalDone,
        Pool<ByteBuffer> byteBuffers,
        Pool<byte[]> buffers,
        AtomicInteger reads
    ) {
        this.fileShape = Objects.requireNonNull(fileShape, "fileShape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.byteBuffer = Objects.requireNonNull(byteBuffer, "byteBuffer");
        this.offset = Non.negative(offset, "offset");
        this.length = Non.negative(length, "length");
        this.total = Non.negativeOrZero(total, "total");
        this.signalDone = signalDone;

        this.byteBuffers = byteBuffers == null ? Pool.byteBuffers(length) : byteBuffers;
        this.buffers = buffers == null ? Pool.byteArrays(length) : buffers;
        this.reads = reads == null
            ? new AtomicInteger(1)
            : reads;
    }

    @Override
    public void completed(Integer result, List<ByteIndexEstimator> estimators) {
        int read = result == null ? 0 : result;
        if (read < 0) {
            return;
        }
        if (read == 0) {
            channel.read(byteBuffer, offset, estimators, this);
        } else {
            try {
                fork(estimators, read);
                estimators.add(estimator(read));
                if (reads.decrementAndGet() == 0) {
                    this.signalDone.run();
                }
            } finally {
                byteBuffers.release(this.byteBuffer);
            }
        }
    }

    @Override
    public void failed(Throwable exc, List<ByteIndexEstimator> estimators) {
        throw new IllegalStateException("Failed @ " + estimators, exc);
    }

    private void fork(List<ByteIndexEstimator> attachment, int read) {
        long nextPosition = offset + read;
        ByteBuffer nextByteBuffer = byteBuffers.acquire();
        if (nextPosition != total) {
            CompletionHandler<Integer, List<ByteIndexEstimator>> handler = new LineCountingCompletionHandler(
                fileShape, partitioning,
                channel,
                nextByteBuffer,
                nextPosition,
                length,
                total,
                signalDone,
                byteBuffers,
                buffers,
                reads
            );
            reads.incrementAndGet();
            channel.read(nextByteBuffer, nextPosition, attachment, handler);
        }
    }

    private ByteIndexEstimator estimator(int read) {
        byte[] buffer = buffers.acquire();
        ByteIndexEstimator byteIndexEstimator = new ByteIndexEstimator(fileShape, partitioning);
        try {
            int actualLength = Math.min(read, buffer.length);
            this.byteBuffer.get(0, buffer, 0, actualLength);
            for (int i = 0; i < actualLength; i++) {
                if (buffer[i] == '\n') {
                    byteIndexEstimator.lineAt(i);
                }
            }
            return byteIndexEstimator;
        } finally {
            buffers.release(buffer);
        }
    }
}
