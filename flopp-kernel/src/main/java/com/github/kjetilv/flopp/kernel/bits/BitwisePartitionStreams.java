package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

final class BitwisePartitionStreams implements PartitionedStreams {

    private final Shape shape;

    private final List<Partition> partitions;

    private final MemorySegmentSource source;

    BitwisePartitionStreams(Shape shape, List<Partition> partitions, MemorySegmentSource source) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions = Non.empty(Objects.requireNonNull(partitions, "partitions"), "partitions")
            .stream()
            .sorted(Comparator.naturalOrder())
            .toList();
        this.source = Objects.requireNonNull(source, "memorySegmentSource");
    }

    @Override
    public Stream<LongSupplier> lineCounters() {
        int count = partitions.size();
        Map<Integer, BitwiseCounter> map =
            new ConcurrentHashMap<>(Maps.mapCapacity(count));
        return IntStream.range(0, count)
            .mapToObj(index ->
                counterFor(map, index))
            .map(counter ->
                counter::count);
    }

    @Override
    public Stream<? extends PartitionStreamer> streamers(boolean immutable) {
        int count = partitions.size();
        Map<Integer, BitwisePartitionStreamer> map =
            new ConcurrentHashMap<>(Maps.mapCapacity(count));
        return IntStream.range(0, count)
            .mapToObj(index ->
                streamerFor(immutable, map, index));
    }

    private BitwisePartitionStreamer streamerFor(
        boolean immutable,
        Map<Integer, BitwisePartitionStreamer> map,
        int index
    ) {
        return map.computeIfAbsent(index, _ ->
            new BitwisePartitionStreamer(
                partitions.get(index),
                shape,
                source,
                nextSupplier(index, map, immutable),
                immutable
            ));
    }

    private Supplier<BitwisePartitionStreamer> nextSupplier(
        int index, Map<Integer, BitwisePartitionStreamer> map, boolean immutable
    ) {
        int nextIndex = index + 1;
        return nextIndex < partitions.size()
            ? () -> streamerFor(immutable, map, nextIndex)
            : null;
    }

    private BitwiseCounter counterFor(
        Map<Integer, BitwiseCounter> map,
        int index
    ) {
        return map.computeIfAbsent(index, _ -> new BitwiseCounter(
            partitions.get(index),
            source,
            index + 1 < partitions.size()
                ? () -> counterFor(map, index)
                : null
        ));
    }
}
