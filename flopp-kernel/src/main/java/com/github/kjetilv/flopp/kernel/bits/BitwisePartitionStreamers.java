package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class BitwisePartitionStreamers implements Closeable {

    private final Path path;

    private final Partitioning partitioning;

    private final Shape shape;

    private final FileChannel fileChannel;

    private final Arena arena;

    private final RandomAccessFile randomAccessFile;

    public BitwisePartitionStreamers(Path path, Partitioning partitioning, Shape shape) {
        this.path = path;
        this.partitioning = partitioning;
        this.shape = shape;
        try {
            randomAccessFile = new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} could not access file", e);
        }
        this.fileChannel = randomAccessFile.getChannel();
        this.arena = Arena.ofAuto();
    }

    public Stream<BitwisePartitionStreamer> streamers() {
        return partitioning.of(shape.size())
            .stream()
            .map(partition ->
                new BitwisePartitionStreamer(
                    partition,
                    segment(partition)
                ));
    }

    @Override
    public void close() {
        try {
            randomAccessFile.close();
            fileChannel.close();
        } catch (Exception e) {
            throw new RuntimeException(STR."\{this} could not close", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}/\{shape}/\{partitioning}]";
    }

    private MemorySegment segment(Partition partition) {
        long offset = partition.offset();
        long length = length(partition);
        try {
            return fileChannel.map(READ_ONLY, offset, length, arena);
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to stream partition \{partition}", e);
        }
    }

    private long length(Partition partition) {
        return Math.min(
            shape.size() - partition.offset(),
            partition.bufferedTo(shape.stats().longestLine() + 1)
        );
    }
}
