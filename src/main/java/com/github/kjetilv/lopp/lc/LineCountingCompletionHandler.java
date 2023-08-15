package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.Non;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

final class LineCountingCompletionHandler implements CompletionHandler<Integer, LongAdder> {

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
            AsynchronousFileChannel channel,
            ByteBuffer byteBuffer,
            long offset,
            int length,
            long total,
            Runnable signalDone
    ) {
        this(channel, byteBuffer, offset, length, total, signalDone, null, null, null);
    }

    private LineCountingCompletionHandler(
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
    public void completed(Integer result, LongAdder linesCount) {
        int read = result == null ? 0 : result;
        if (read < 0) {
            return;
        }
        if (read == 0) {
            channel.read(byteBuffer, offset, linesCount, this);
        } else {
            try {
                fork(linesCount, read);
                long newLinesFound = countLines(read);
                linesCount.add(newLinesFound);
                if (reads.decrementAndGet() == 0) {
                    this.signalDone.run();
                }
            } finally {
                byteBuffers.release(this.byteBuffer);
            }
        }
    }

    private void fork(LongAdder attachment, int read) {
        long nextPosition = offset + read;
        if (nextPosition != total) {
            ByteBuffer nextByteBuffer = byteBuffers.acquire();
            CompletionHandler<Integer, LongAdder> handler = new LineCountingCompletionHandler(
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

    private long countLines(int read) {
        byte[] buffer = buffers.acquire();
        try {
            long newLinesFound = 0;
            int actualLength = Math.min(read, buffer.length);
            this.byteBuffer.get(0, buffer, 0, actualLength);
            for (int i = 0; i < actualLength; i++) {
                if (buffer[i] == '\n') {
                    newLinesFound++;
                }
            }
            return newLinesFound;
        } finally {
            buffers.release(buffer);
        }
    }

    @Override
    public void failed(Throwable exc, LongAdder attachment) {
        throw new IllegalStateException("Failed @ " + attachment, exc);
    }
}
