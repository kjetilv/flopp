package com.github.kjetilv.flopp.lc;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import com.github.kjetilv.flopp.Non;

final class LineCountingCompletionHandler implements CompletionHandler<Integer, LongAdder> {

    private final AsynchronousFileChannel channel;

    private final ByteBuffer byteBuffer;

    private final long offset;

    private final int bufferSize;

    private final long size;

    private final Runnable signalDone;

    private final Pool<ByteBuffer> byteBuffers;

    private final Pool<byte[]> buffers;

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
            Pool<ByteBuffer> byteBuffers,
            Pool<byte[]> buffers,
            LongAdder bytesRead
    ) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.byteBuffer = Objects.requireNonNull(byteBuffer, "byteBuffer");
        this.offset = Non.negative(offset, "offset");
        this.bufferSize = Non.negative(bufferSize, "length");
        this.size = Non.negativeOrZero(size, "total");
        this.signalDone = signalDone;

        this.byteBuffers = byteBuffers == null ? Pool.byteBuffers(bufferSize) : byteBuffers;
        this.buffers = buffers == null ? Pool.byteArrays(bufferSize) : buffers;
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
                linesCount.add(countLines(read));
            } finally {
                byteBuffers.release(this.byteBuffer);
                if (bytesRead.longValue() == size) {
                    this.signalDone.run();
                }
            }
        }
    }

    private void fork(LongAdder linesCount, long position) {
        ByteBuffer nextByteBuffer = byteBuffers.acquire();
        LineCountingCompletionHandler forkedHandler = forked(nextByteBuffer, position);
        channel.read(nextByteBuffer, position, linesCount, forkedHandler);
    }

    private LineCountingCompletionHandler forked(ByteBuffer nextByteBuffer, long nextPosition) {
        return new LineCountingCompletionHandler(
            channel,
            nextByteBuffer,
            nextPosition,
            bufferSize,
            size,
            signalDone,
            byteBuffers,
            buffers,
            bytesRead
        );
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
