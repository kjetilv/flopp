package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.formats.HeadersAndFooters;
import com.github.kjetilv.flopp.kernel.util.CloseableConsumer;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class VectorPartitionLineSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final HeadersAndFooters<LineSegment> headersAndFooters;

    private final MemorySegment segment;

    private final long offset;

    private final long logicalLimit;

    VectorPartitionLineSpliterator(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalLimit,
        HeadersAndFooters<LineSegment> headersAndFooters
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.offset = offset;
        this.logicalLimit = logicalLimit;
        this.headersAndFooters = headersAndFooters;
        this.segment = segment;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Consumer<LineSegment> consumer = (Consumer<LineSegment>) action;
            if (headersAndFooters == null) {
                feeder(consumer).run();
            } else {
                try (CloseableConsumer<LineSegment> hf = headersAndFooters.wrap(consumer)) {
                    feeder(hf == null ? consumer : hf).run();
                }
            }
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed: " + action, e);
        }
    }

    private VectorPartitionLineFeeder feeder(Consumer<LineSegment> action) {
        return new VectorPartitionLineFeeder(partition, segment, offset, logicalLimit, action);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }
}
