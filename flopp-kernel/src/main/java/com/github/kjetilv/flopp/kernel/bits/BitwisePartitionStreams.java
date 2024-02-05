package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

final class BitwisePartitionStreams implements PartitionedStreams {

    private final Path path;

    private final Shape shape;

    private final List<Partition> partitions;

    private final MemorySegmentSource memorySegmentSource;

    BitwisePartitionStreams(Path path, Shape shape, List<Partition> partitions) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions =
            Non.empty(Objects.requireNonNull(partitions, "partitions"), "partitions");

        this.memorySegmentSource = new MemorySegmentSource(this.path, this.shape);
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
