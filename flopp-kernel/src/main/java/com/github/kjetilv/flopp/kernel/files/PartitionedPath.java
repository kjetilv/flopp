package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.util.AtomicArray;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

final class PartitionedPath implements Partitioned<Path> {

    private final Path path;

    private final Shape shape;

    private final Partitions partitions;

    private final MemorySegmentSource memorySegmentSource;

    PartitionedPath(Path path, Partitioning partitioning, Shape shape) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = shape == null ? Shape.of(path) : shape;
        this.partitions = partitioning(partitioning, this.shape).of(this.shape.size());
        this.memorySegmentSource = new PartialMemorySegmentSource(this.path, this.shape);
    }

    @Override
    public Path partitioned() {
        return path;
    }

    @Override
    public Partitions partitions() {
        return partitions;
    }

    @Override
    public Stream<PartitionResult<Void>> forEachLine(
        BiConsumer<Partition, Stream<LineSegment>> consumer
    ) {
        return streamers().map(partitionStreamer ->
            new PartitionResult<>(
                partitionStreamer.partition(), () -> {
                consumer.accept(partitionStreamer.partition(), partitionStreamer.lines());
                return null;
            }
            ));
    }

    @Override
    public <T> Stream<PartitionResult<T>> map(BiFunction<Partition, Stream<LineSegment>, T> processor) {
        return streamers().map(streamer ->
            new PartitionResult<>(
                streamer.partition(), () ->
                processor.apply(streamer.partition(), streamer.lines())
            ));
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
    public Stream<? extends PartitionStreamer> streamers() {
        int count = partitions.size();
        AtomicArray<BitwisePartitionStreamer> array = new AtomicArray<>(count);
        return IntStream.range(0, count)
            .mapToObj(lazyStreamer(array));
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

    private Supplier<BitwisePartitionStreamer> nextLookup(
        int index,
        AtomicArray<BitwisePartitionStreamer> array
    ) {
        int nextIndex = index + 1;
        return nextIndex < partitions.size()
            ? () -> streamerFor(nextIndex, array)
            : null;
    }

    private IntFunction<BitwisePartitionStreamer> lazyStreamer(
        AtomicArray<BitwisePartitionStreamer> array
    ) {
        return index -> streamerFor(index, array);
    }

    private BitwisePartitionStreamer streamerFor(
        int index,
        AtomicArray<BitwisePartitionStreamer> array
    ) {
        return array.computeIfAbsent(
            index, () -> {
                Partition partition = partitions.get(index);
                return new BitwisePartitionStreamer(partition, shape, memorySegmentSource, nextLookup(index, array));
            }
        );
    }

    private BitwiseCounter counterFor(
        Map<Integer, BitwiseCounter> map,
        int index
    ) {
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
            partitioning == null ? Partitioning.create() : partitioning,
            shape
        );
    }

    private static Partitioning withTail(Partitioning partitioning, Shape shape) {
        return partitioning.tail() == 0 && shape.limitsLineLength()
            ? partitioning.tail(shape.longestLine())
            : partitioning;
    }

    @FunctionalInterface
    public interface Action extends Closeable, Consumer<LineSegment> {

        default void close() {
        }
    }
}
