package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.Partitions;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

final class BitwisePartitionStreams implements PartitionedStreams {

    private final Shape shape;

    private final Partitions partitions;

    private final MemorySegmentSource source;

    BitwisePartitionStreams(Shape shape, Partitions partitions, MemorySegmentSource source) {
        this.shape = shape;
        this.partitions = partitions;
        this.source = source;
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
        ConcurrentMap<Integer, BitwisePartitionStreamer> map = new ConcurrentHashMap<>(Maps.mapCapacity(count));
        return IntStream.range(0, count)
            .mapToObj(index ->
                streamerFor(index, map));
    }

    private BitwisePartitionStreamer streamerFor(
        int index,
        ConcurrentMap<Integer, BitwisePartitionStreamer> map
    ) {
        return map.computeIfAbsent(index, _ ->
            new BitwisePartitionStreamer(
                partitions.get(index),
                shape,
                source,
                nextLookup(index, map)
            ));
    }

    private Supplier<BitwisePartitionStreamer> nextLookup(
        int index,
        ConcurrentMap<Integer, BitwisePartitionStreamer> map
    ) {
        int nextIndex = index + 1;
        return nextIndex < partitions.size()
            ? () -> streamerFor(nextIndex, map)
            : null;
    }

    private BitwiseCounter counterFor(
        Map<Integer, BitwiseCounter> map,
        int index
    ) {
        return map.computeIfAbsent(index, _ ->
            new BitwiseCounter(
                partitions.get(index),
                source,
                index + 1 < partitions.size()
                    ? () -> counterFor(map, index)
                    : null
            ));
    }
}
