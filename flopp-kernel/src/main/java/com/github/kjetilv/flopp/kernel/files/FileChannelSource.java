package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.Partition;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

final class FileChannelSource implements ByteSource {

    private final MappedByteBuffer mappedByteBuffer;

    FileChannelSource(Partition partition, FileChannel channel, long size) {
        this.mappedByteBuffer = mappedByteBuffer(partition, channel, size);
    }

    @Override
    public int fill(byte[] bytes, int offset, int length) {
        int bytesToRead = Math.min(length, mappedByteBuffer.limit() - offset);
        mappedByteBuffer.get(offset, bytes, 0, bytesToRead);
        return bytesToRead;
    }

    private static final int DEFAULT_LONGEST_LINE = 1024;

    private static MappedByteBuffer mappedByteBuffer(
        Partition partition,
        FileChannel channel,
        long size
    ) {
        long traverseLimit = Math.min(
            partition.count() + (partition.last() ? 0 : DEFAULT_LONGEST_LINE),
            size - partition.offset()
        );
        try {
            return channel.map(
                FileChannel.MapMode.READ_ONLY,
                partition.offset(),
                traverseLimit
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + channel, e);
        }
    }
}
