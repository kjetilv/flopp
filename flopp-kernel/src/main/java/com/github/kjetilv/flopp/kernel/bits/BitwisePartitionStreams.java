package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

final class BitwisePartitionStreams implements PartitionedStreams {

    private final Shape shape;

    private final List<Partition> partitions;

    private final MemorySegmentSource source;

    BitwisePartitionStreams(Shape shape, List<Partition> partitions, MemorySegmentSource source) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions = Non.empty(Objects.requireNonNull(partitions, "partitions"), "partitions");
        this.source = Objects.requireNonNull(source, "memorySegmentSource");
    }

    @Override
    public List<? extends PartitionStreamer> streamersList(boolean immutable) {
        return windup(
            new LinkedList<>(partitions),
            (partition, streamer) -> new BitwisePartitionStreamer(
                partition,
                shape,
                source,
                streamer,
                immutable
            ),
            new LinkedList<BitwisePartitionStreamer>()
        );
    }

    @Override
    public List<LongSupplier> lineCountersList() {
        return windup(
            new LinkedList<>(partitions),
            (partition, counter) ->
                new BitwiseCounter(partition, source, counter),
            new LinkedList<BitwiseCounter>()
        ).stream()
            .map(BitwisePartitionStreams::counter)
            .toList();
    }

    @Override
    public List<Consumer<Consumer<SeparatedLine>>> lineSplittersList(
        LinesFormat linesFormat,
        boolean immutable
    ) {
        return streamers(immutable)
            .map(streamer ->
                (Consumer<Consumer<SeparatedLine>>) consumer ->
                    streamer.lines()
                        .forEach(new BitwiseLineSplitter(
                            linesFormat,
                            consumer
                        )))
            .toList();
    }

    private static LongSupplier counter(BitwiseCounter counter) {
        return counter::count;
    }

    private static <T> List<T> windup(
        List<Partition> partitions,
        BiFunction<Partition, T, T> function,
        List<T> target
    ) {
        Partition head = partitions.removeFirst();
        if (!partitions.isEmpty()) {
            windup(partitions, function, target);
        }
        T nextT = target.isEmpty() ? null : target.getFirst();
        target.addFirst(function.apply(head, nextT));
        return target;
    }
}
