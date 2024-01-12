package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

final class FileChannelByteSource implements ByteSource {

    private final Partition partition;

    private final FileChannel channel;

    private final RandomAccessFile randomAccessFile;

    private final long fileSize;

    private final int padding;

    private MappedByteBuffer mappedByteBuffer;

    private int bytesReadTotal;

    private int bytesReadInBuffer;

    FileChannelByteSource(Partition partition, RandomAccessFile randomAccessFile, long fileSize, int padding) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.randomAccessFile = Objects.requireNonNull(randomAccessFile, "randomAccessFile");
        this.channel = randomAccessFile.getChannel();
        this.fileSize = Non.negative(fileSize, "fileSize");
        this.padding = Non.negativeOrZero(padding, "padding");
        newBuffer(this.partition.offset(), Math.min(
            this.partition.count() + padding,
            this.fileSize - this.partition.offset()
        ));
    }

    @Override
    public int fill(byte[] bytes) {
        if (bytesReadTotal >= fileSize) {
            return -1;
        }
        if (mappedByteBuffer == null) {
            long bytesLeftTotal = fileSize - bytesReadTotal;
            long limit = Math.min(padding, bytesLeftTotal);
            newBuffer(bytesReadTotal, limit);
            this.bytesReadInBuffer = 0;
        }
        int bufferLimit = mappedByteBuffer.limit();
        int bytesToRead = Math.min(
            bufferLimit - bytesReadInBuffer,
            bytes.length
        );
        get(bytes, bytesToRead);
        if (bytesReadInBuffer >= bufferLimit) {
            mappedByteBuffer = null;
        }
        return bytesToRead;
    }

    @Override
    public void close() {
        try {
            this.channel.close();
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} failed to close: \{channel}", e);
        }
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close: \{randomAccessFile}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}/f:\{fileSize}/p:\{padding}]";
    }

    private void get(byte[] bytes, int count) {
        try {
            mappedByteBuffer.get(bytesReadInBuffer, bytes, 0, count);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to get \{bytesReadTotal}/\{count} bytes", e);
        } finally {
            bytesReadInBuffer += count;
            bytesReadTotal += count;
        }
    }

    private void newBuffer(long pos, long size) {
        try {
            this.mappedByteBuffer = channel.map(READ_ONLY, pos, size);
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to open \{channel}", e);
        }
    }
}
