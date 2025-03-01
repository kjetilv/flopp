package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import com.github.kjetilv.flopp.kernel.partitions.Partitionings;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

final class VectorPartitioned implements Partitioned {

    private final Shape shape;

    private final Partitions partitions;

    private final MemorySegmentSource memorySegmentSource;

    VectorPartitioned(Partitioning partitioning, Shape shape, MemorySegmentSource segmentSource) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions = partitioning(partitioning, this.shape).of(this.shape.size());
        this.memorySegmentSource = segmentSource;
    }

    @Override
    public Partitions partitions() {
        return partitions;
    }

    @Override
    public Stream<LongSupplier> lineCounters() {
        int count = partitions.size();
        Map<Integer, BitwiseCounter> map = new ConcurrentHashMap<>(Maps.mapCapacity(count));
        return IntStream.range(0, count)
            .mapToObj(index ->
                counterFor(map, index))
            .map(counter ->
                counter::count);
    }

    @Override
    public Stream<PartitionStreamer> streamers() {
        int count = partitions.size();
        return IntStream.range(0, count).mapToObj(index ->
            new VectorPartitionStreamer(partitions.get(index), shape, memorySegmentSource));
    }

    @Override
    public Stream<PartitionedSplitter> splitters(Format format) {
        return switch (format) {
            case Format.Csv csv -> streamers()
                .map(streamer ->
                    new BitwiseCsvSplitter(streamer, csv));
            case Format.FwFormat fw -> streamers()
                .map(streamer ->
                    new BitwiseFwSplitter(streamer, fw));
        };
    }

    @Override
    public void close() {
        try {
            memorySegmentSource.close();
        } catch (Exception e) {
            throw new RuntimeException(this + " could not close", e);
        }
    }

    private BitwiseCounter counterFor(Map<Integer, BitwiseCounter> map, int index) {
        return map.computeIfAbsent(
            index, _ ->
                new BitwiseCounter(
                    partitions.get(index),
                    memorySegmentSource,
                    index + 1 < partitions.size()
                        ? () -> counterFor(map, index)
                        : null
                )
        );
    }

    private static Partitioning partitioning(Partitioning partitioning, Shape shape) {
        return withTail(
            partitioning == null ? Partitionings.create() : partitioning,
            shape
        );
    }

    private static Partitioning withTail(Partitioning partitioning, Shape shape) {
        return partitioning.tail() == 0 && shape.limitsLineLength()
            ? partitioning.tail(shape.longestLine())
            : partitioning;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + shape + "/" + partitions + " <- " + memorySegmentSource + "]";
    }
}
