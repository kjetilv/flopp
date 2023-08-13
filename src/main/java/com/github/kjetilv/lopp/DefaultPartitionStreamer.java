package com.github.kjetilv.lopp;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class DefaultPartitionStreamer implements PartitionStreamer {

    private final RandomAccessFile randomAccessFile;

    private final PartitionBytes partitionBytes;

    private final int sliceSize;

    private final FileShape fileShape;

    DefaultPartitionStreamer(
        RandomAccessFile randomAccessFile,
        PartitionBytes partitionBytes,
        int sliceSize,
        FileShape fileShape
    ) {
        this.randomAccessFile = Objects.requireNonNull(randomAccessFile, "randomAccessFile");
        this.partitionBytes = Objects.requireNonNull(partitionBytes, "partition");
        this.sliceSize = sliceSize > 0 ? sliceSize : DEFAULT_SLICE_SIZE;
        this.fileShape = Objects.requireNonNull(fileShape, "shape");
    }

    public PartitionBytes partitionBytes() {
        return partitionBytes;
    }

    @Override
    public Partition partition() {
        return partitionBytes.partition();
    }

    @Override
    @SuppressWarnings("ChannelOpenedButNotSafelyClosed")
    public Stream<NPLine> lines() {
        FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer;
        try {
            mappedByteBuffer = channel.map(READ_ONLY, partitionBytes.offset(), partitionBytes.count());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open partition " + partitionBytes, e);
        }
        return StreamSupport.stream(
            new PartitionSpliterator(mappedByteBuffer, partitionBytes, fileShape, sliceSize),
            false);
    }

    private static final int DEFAULT_SLICE_SIZE = 8192;

    @Override public String toString() {
        return getClass().getSimpleName() + "[" + partitionBytes + "]";
    }
}
