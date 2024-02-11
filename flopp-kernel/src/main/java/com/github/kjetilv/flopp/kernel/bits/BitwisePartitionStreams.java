package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

final class BitwisePartitionStreams implements PartitionedStreams {

    private final Path path;

    private final Shape shape;

    private final List<Partition> partitions;

    private final Function<Partition, MemorySegment> memorySegmentSource;

    BitwisePartitionStreams(
        Path path,
        Shape shape,
        List<Partition> partitions,
        Function<Partition, MemorySegment> memorySegmentSource
    ) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions = Non.empty(Objects.requireNonNull(partitions, "partitions"), "partitions");
        this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
    }

    @Override
    public List<? extends PartitionStreamer> streamersList(boolean copying) {
        return BitwisePartitionStreams.<BitwisePartitionStreamer>buildUp(
            new LinkedList<>(partitions),
            (partition, streamer) ->
                new BitwisePartitionStreamer(partition, shape, memorySegmentSource, streamer, copying)
        );
    }

    @Override
    public List<LongSupplier> lineCountersList() {
        return BitwisePartitionStreams.<BitwiseCounter>buildUp(
                new LinkedList<>(partitions),
                (partition, counter) ->
                    new BitwiseCounter(partition, memorySegmentSource, counter)
            )
            .stream()
            .map(BitwisePartitionStreams::counter)
            .toList();
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}/\{shape}/ partitions:\{partitions.size()}]";
    }

    private static LongSupplier counter(BitwiseCounter counter) {
        return counter::count;
    }

    private static <T> LinkedList<T> buildUp(
        LinkedList<Partition> partitionsTail,
        BiFunction<Partition, T, T> function
    ) {
        if (partitionsTail.isEmpty()) {
            return new LinkedList<>();
        }
        Partition head = partitionsTail.removeFirst();
        LinkedList<T> ts = buildUp(partitionsTail, function);
        T nextT = ts.isEmpty() ? null : ts.getFirst();
        ts.addFirst(function.apply(head, nextT));
        return ts;
    }
}
