package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.HeadersAndFooters;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.kjetilv.flopp.kernel.formats.HeadersAndFooters.create;

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

        HeadersAndFooters<LineSegment> headersAndFooters =
            create(partition, shape, LineSegment::immutable);
        this.spliterator = new BitwisePartitionLineSpliterator(
            partition,
            safeSegment,
            offset,
            logicalSize,
            headersAndFooters,
            nextSupplier
        );
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public Stream<LineSegment> lines() {
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }
}
