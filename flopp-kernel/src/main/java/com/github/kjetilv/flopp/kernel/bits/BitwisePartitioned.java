package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;
import com.github.kjetilv.flopp.kernel.util.AtomicArray;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("preview")
final class BitwisePartitioned implements Partitioned<Path> {

    private final Path path;

    private final Shape shape;

    private final Partitions partitions;

    private final MemorySegmentSource memorySegmentSource;

    BitwisePartitioned(Path path, Partitioning partitioning, Shape shape) {
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
    public PartitionedProcessor<LineSegment, String> processor(Path target) {
        return (processor, executor) -> {
            LinesWriterFactory<Path, String> writers = path ->
                new MemoryMappedByteArrayLinesWriter(path, BUFFER_SIZE, shape.charset());
            ResultCollector<Path> collector =
                new ResultCollector<>(partitions.size(), sizer(), executor);
            try (
                TempTargets<Path> tempTargets = new TempDirTargets(path);
                Transfers<Path> transfers = new FileChannelTransfers(target)
            ) {
                try (StructuredTaskScope<PartitionResult<Path>> scope = new StructuredTaskScope<>()) {
                    streamers()
                        .forEach(streamer ->
                            scope.fork(() -> {
                                Path tempTarget = tempTargets.temp(streamer.partition());
                                try (LinesWriter<String> writer = writers.create(tempTarget)) {
                                    streamer.lines()
                                        .map(processor.apply(streamer.partition()))
                                        .forEach(writer);
                                }
                                collector.sync(new PartitionResult<>(streamer.partition(), tempTarget));
                                return null;
                            }));
                    try {
                        scope.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted", e);
                    }
                }
                collector.syncTo(transfers);
            }
        };
    }

    @Override
    public PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processor(Path target, Format format) {
        return (processor, executor) -> {
            LinesWriterFactory<Path, Stream<LineSegment>> writers = path ->
                new LineSegmentsWriter(path, MEMORY_SEGMENT_SIZE);
            ResultCollector<Path> collector = new ResultCollector<>(partitions.size(), sizer(), executor);
            try (
                TempTargets<Path> tempTargets = new TempDirTargets(path);
                Transfers<Path> transfers = new FileChannelTransfers(target)
            ) {
                try (StructuredTaskScope<PartitionResult<Path>> scope = new StructuredTaskScope<>()) {
                    this.splitters(format)
                        .forEach(splitter ->
                            scope.fork(() -> {
                                Path tempTarget = tempTargets.temp(splitter.partition());
                                try (LinesWriter<Stream<LineSegment>> writer = writers.create(tempTarget)) {
                                    splitter.separatedLines()
                                        .map(processor.apply(splitter.partition()))
                                        .forEach(writer);
                                }
                                collector.sync(new PartitionResult<>(splitter.partition(), tempTarget));
                                return null;
                            }));
                    try {
                        scope.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted", e);
                    }
                }
                collector.syncTo(transfers);
            }
        };
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<LineSegment>> consumer
    ) {
        return streamers()
            .map(partitionStreamer ->
                CompletableFuture.supplyAsync(() -> new PartitionResult<>(
                    partitionStreamer.partition(),
                    getAccept(consumer, partitionStreamer)
                )));
    }

    @Override
    public <T> Stream<CompletableFuture<PartitionResult<T>>> map(
        BiFunction<Partition, Stream<LineSegment>, T> processor,
        ExecutorService executorService
    ) {
        return streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(
                        () ->
                            processor.apply(streamer.partition(), streamer.lines()),
                        executorService
                    )
                    .thenApply(result ->
                        new PartitionResult<>(streamer.partition(), result)));
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
            .mapToObj(index ->
                streamerFor(index, array));
    }

    @Override
    public Stream<? extends CompletableFuture<PartitionStreamer>> streamers(ExecutorService executorService) {
        int count = partitions.size();
        AtomicArray<BitwisePartitionStreamer> array = new AtomicArray<>(count);
        return IntStream.range(0, count)
            .mapToObj(index ->
                CompletableFuture.supplyAsync(
                    () -> streamerFor(index, array),
                    executorService
                ));
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
    public Stream<CompletableFuture<PartitionedSplitter>> splitters(
        Format format,
        ExecutorService executorService
    ) {
        return switch (format) {
            case Format.Csv csv -> streamers(executorService)
                .map(future ->
                    future.thenApply(streamer ->
                        new BitwiseCsvSplitter(streamer, csv)));
            case Format.FwFormat fw -> streamers(executorService)
                .map(future ->
                    future.thenApply(streamer ->
                        new BitwiseFwSplitter(streamer, fw)));
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

    private Supplier<BitwisePartitionStreamer> nextLookup(
        int index,
        AtomicArray<BitwisePartitionStreamer> array
    ) {
        int nextIndex = index + 1;
        return nextIndex < partitions.size()
            ? () -> streamerFor(nextIndex, array)
            : null;
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

    private static final int BUFFER_SIZE = 8192;

    private static final int MEMORY_SEGMENT_SIZE = 8 * BUFFER_SIZE;

    private static ToLongFunction<Path> sizer() {
        return path -> Shape.of(path).size();
    }

    private static Partitioning partitioning(Partitioning partitioning, Shape shape) {
        return withTail(
            partitioning == null ? Partitioning.create() : partitioning,
            shape
        );
    }

    private static Partitioning withTail(Partitioning partitioning, Shape shape) {
        if (partitioning.tail() == 0 && shape.limitsLineLength()) {
            return partitioning.tail(shape.longestLine());
        }
        return partitioning;
    }

    private static Void getAccept(
        BiConsumer<Partition, Stream<LineSegment>> consumer,
        PartitionStreamer partitionStreamer
    ) {
        consumer.accept(partitionStreamer.partition(), partitionStreamer.lines());
        return null;
    }

    @FunctionalInterface
    public interface Action extends Closeable, Consumer<LineSegment> {

        default void close() {
        }
    }
}
