package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.Partitions;
import com.github.kjetilv.flopp.kernel.formats.Shape;
import com.github.kjetilv.flopp.kernel.util.AtomicArray;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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

    private BitwisePartitionStreamer streamerFor(
        int index,
        AtomicArray<BitwisePartitionStreamer> array
    ) {
        return array.computeIfAbsent(index, () -> {
            Partition partition = partitions.get(index);
            return new BitwisePartitionStreamer(partition, shape, source, nextLookup(index,  array));
        });
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
