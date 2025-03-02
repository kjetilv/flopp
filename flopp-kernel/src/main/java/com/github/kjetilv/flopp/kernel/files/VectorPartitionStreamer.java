package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.HeadersAndFooters;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.kjetilv.flopp.kernel.formats.HeadersAndFooters.create;

final class VectorPartitionStreamer implements PartitionStreamer {

    private final Partition partition;

    private final Spliterator<LineSegment> spliterator;

    VectorPartitionStreamer(
        Partition partition,
        Shape shape,
        MemorySegmentSource memorySegmentSource
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        LineSegment sourced = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource")
            .get(partition);

        long logicalLimit = sourced.endIndex();

        MemorySegment paddedMemorySegment = sourced.memorySegment();

        HeadersAndFooters<LineSegment> headersAndFooters = create(partition, shape, LineSegment::immutable);

        this.spliterator = new VectorPartitionLineSpliterator(
            partition,
            paddedMemorySegment,
            sourced.startIndex(),
            logicalLimit,
            headersAndFooters
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
