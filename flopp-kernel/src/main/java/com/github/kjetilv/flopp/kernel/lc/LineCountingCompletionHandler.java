package com.github.kjetilv.flopp.kernel.lc;

import com.github.kjetilv.flopp.kernel.Bits;
import com.github.kjetilv.flopp.kernel.Non;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

final class LineCountingCompletionHandler implements CompletionHandler<Integer, LongAdder> {

    private final AsynchronousFileChannel channel;

    private final ByteBuffer byteBuffer;

    private final long offset;

    private final int bufferSize;

    private final long size;

    private final Runnable signalDone;

    private final QueuePool<ByteBuffer> byteBuffers;

    private final QueuePool<byte[]> buffers;

    private final LongAdder bytesRead;

    LineCountingCompletionHandler(
        AsynchronousFileChannel channel,
        ByteBuffer byteBuffer,
        long offset,
        int bufferSize,
        long size,
        Runnable signalDone
    ) {
        this(channel, byteBuffer, offset, bufferSize, size, signalDone, null, null, null);
    }

    private LineCountingCompletionHandler(
        AsynchronousFileChannel channel,
        ByteBuffer byteBuffer,
        long offset,
        int bufferSize,
        long size,
        Runnable signalDone,
        QueuePool<ByteBuffer> byteBuffers,
        QueuePool<byte[]> buffers,
        LongAdder bytesRead
    ) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.byteBuffer = Objects.requireNonNull(byteBuffer, "byteBuffer");
        this.offset = Non.negative(offset, "offset");
        this.bufferSize = Non.negative(bufferSize, "length");
        this.size = Non.negativeOrZero(size, "total");
        this.signalDone = signalDone;

        this.byteBuffers = byteBuffers == null ? QueuePool.byteBuffers(bufferSize) : byteBuffers;
        this.buffers = buffers == null ? QueuePool.byteArrays(bufferSize) : buffers;
        this.bytesRead = bytesRead == null
            ? new LongAdder()
            : bytesRead;
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
            bytesRead.add(read);
            try {
                long nextPosition = offset + read;
                if (nextPosition < size) {
                    fork(linesCount, nextPosition);
                }
                byteBuffer.flip();
                linesCount.add(countLines(read));
            } finally {
                byteBuffers.release(this.byteBuffer);
                if (bytesRead.longValue() == size) {
                    this.signalDone.run();
                }
            }
        }
    }

    private long countLines(int read) {
        long longs = read / ALIGNMENT;
        long newLinesFound = 0;
        for (int i = 0; i < longs; i++) {
            newLinesFound += counter.count(this.byteBuffer.getLong());
        }
        long tail = read % ALIGNMENT;
        for (int i = 0; i < tail; i++) {
            if (this.byteBuffer.get() == '\n') {
                newLinesFound++;
            }
        }
        return newLinesFound;
    }

    @Override
    public void failed(Throwable exc, LongAdder attachment) {
        throw new IllegalStateException(STR."Failed @ \{attachment}", exc);
    }

    private void fork(LongAdder linesCount, long position) {
        ByteBuffer nextByteBuffer = byteBuffers.acquire();
        channel.read(
            nextByteBuffer,
            position,
            linesCount,
            new LineCountingCompletionHandler(
                channel,
                nextByteBuffer,
                position,
                bufferSize,
                size,
                signalDone,
                byteBuffers,
                buffers,
                bytesRead
            )
        );
    }

    private static final Bits.Counter counter = Bits.counter('\n');

    public static final int ALIGNMENT = 8;
}
