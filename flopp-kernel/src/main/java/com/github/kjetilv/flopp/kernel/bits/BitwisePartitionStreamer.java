package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.MemorySegments;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.kjetilv.flopp.kernel.bits.HeadersAndFooters.headersAndFooters;

final class BitwisePartitionStreamer implements PartitionStreamer {

    private final Partition partition;

    private final BitwisePartitionLineSpliterator spliterator;

    BitwisePartitionStreamer(
        Partition partition,
        Shape shape,
        MemorySegmentSource memorySegmentSource,
        Supplier<BitwisePartitionStreamer> next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        LineSegment sourced = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource")
            .get(partition);

        long logicalSize = sourced.length();

        boolean troubledTail = partition.troubledTail();
        long offset = troubledTail ? 0L : sourced.startIndex();
        MemorySegment safeSegment = troubledTail
            ? MemorySegments.alignmentPadded(sourced.memorySegment(), sourced.startIndex())
            : sourced.memorySegment();

        Supplier<BitwisePartitionLineSpliterator> nextSupplier = next == null
            ? null
            : () -> next.get().spliterator;

        this.spliterator = new BitwisePartitionLineSpliterator(
            partition,
            safeSegment,
            offset,
            logicalSize,
            headersAndFooters(partition, shape),
            nextSupplier
        );
    }

    @Override
    public Stream<LineSegment> lines() {
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }
}
