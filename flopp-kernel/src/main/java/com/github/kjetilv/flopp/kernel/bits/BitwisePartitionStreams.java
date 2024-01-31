package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public final class BitwisePartitionStreams implements PartitionedStreams {

    private final Path path;

    private final Shape shape;

    private final List<Partition> partitions;

    private final MemorySegmentSource memorySegmentSource;

    public BitwisePartitionStreams(Path path, Shape shape, List<Partition> partitions) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions = Objects.requireNonNull(partitions, "partitions");
        if (this.partitions.isEmpty()) {
            throw new IllegalArgumentException("No partitions receveid");
        }

        this.memorySegmentSource = new MemorySegmentSource(path, shape, Arena::ofAuto);
    }

    public Stream<PartitionStreamer> streamers() {
        return partitions.stream().map(this::streamer);
    }

    public Stream<CompletableFuture<BitwisePartitionStreamer>> streamers(ExecutorService executor) {
        Objects.requireNonNull(executor, "executor");
        return partitions.stream()
            .map(partition ->
                CompletableFuture.supplyAsync(() -> streamer(partition), executor));
    }

    @Override
    public void close() {
        try {
            memorySegmentSource.close();
        } catch (Exception e) {
            throw new RuntimeException(STR."\{this} could not close", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}/\{shape}/ partitions:\{partitions.size()}]";
    }

    private BitwisePartitionStreamer streamer(Partition partition) {
        return new BitwisePartitionStreamer(partition, shape, memorySegmentSource);
    }
}
