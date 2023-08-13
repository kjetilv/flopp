package com.github.kjetilv.lopp;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class FastPartitionStreamer implements PartitionStreamer {

    private final RandomAccessFile randomAccessFile;

    private final Partition partition;

    private final int sliceSize;

    private final FileShape fileShape;

    FastPartitionStreamer(RandomAccessFile randomAccessFile, Partition partition, FileShape fileShape, int sliceSize) {
        this.randomAccessFile = Objects.requireNonNull(randomAccessFile, "randomAccessFile");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.sliceSize = sliceSize > 0 ? sliceSize : DEFAULT_SLICE_SIZE;
        this.fileShape = Objects.requireNonNull(fileShape, "shape");
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<NPLine> lines() {
        FileChannel channel = randomAccessFile.getChannel();
        return StreamSupport.stream(
            new FastPartitionSpliterator(channel, partition, fileShape, sliceSize),
            false
        );
    }

    private static final int DEFAULT_SLICE_SIZE = 8192;

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }
}
