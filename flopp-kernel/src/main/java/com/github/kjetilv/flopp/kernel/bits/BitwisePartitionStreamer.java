package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.HeaderFooterMediator;
import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class BitwisePartitionStreamer {

    private final Shape shape;

    private final MemorySegment memorySegment;

    private final Partition partition;

    public BitwisePartitionStreamer(
        Partition partition,
        Shape shape,
        MemorySegment memorySegment
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.shape = shape;
        this.memorySegment = memorySegment;
    }

    public Stream<LineSegment> lines() {
        Mediator<LineSegment> mediator = mediator();
        return StreamSupport.stream(
            new BitwisePartitionSpliterator2(partition, memorySegment, mediator),
            false
        );
    }

    public Partition partition() {
        return partition;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    private Mediator<LineSegment> mediator() {
        boolean overhead = shape != null && shape.hasOverhead();
        if (overhead) {
            boolean first = partition.first();
            boolean last = partition.last();
            if (first && last) {
                return new HeaderFooterMediator<>(shape.header(), shape.footer(), LineSegment::immutable);
            }
            if (first) {
                return new HeaderFooterMediator<>(shape.header());
            }
            if (last) {
                return new HeaderFooterMediator<>(shape.footer(), LineSegment::immutable);
            }
        }
        return null;
    }
}
