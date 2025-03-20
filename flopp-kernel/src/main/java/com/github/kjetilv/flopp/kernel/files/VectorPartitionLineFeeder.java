package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Vectors;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

final class VectorPartitionLineFeeder implements Runnable, LineSegment {

    private final Partition partition;

    private final MemorySegment segment;

    private final Vectors.Finder finder;

    private final Consumer<LineSegment> action;

    private final long logicalLimit;

    private long startIndex;

    private long endIndex;

    VectorPartitionLineFeeder(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalLimit,
        Consumer<LineSegment> action
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = Objects.requireNonNull(segment, "segment");
        this.finder = Vectors.finder(
            this.segment,
            Non.negative(offset, "offset"),
            (byte) '\n'
        );
        this.logicalLimit = logicalLimit;
        this.action = Objects.requireNonNull(action, "action");
    }

    @Override
    public void run() {
        long position;
        if (partition.first()) {
            position = 0L;
            startIndex = 0L;
        } else {
            position = finder.getAsLong();
            if (position < 0L) {
                return;
            }
            startIndex = position + 1;
        }
        try {
            while (startIndex <= logicalLimit && (position = finder.getAsLong()) >= 0L) {
                endIndex = position;
                action.accept(this);
                startIndex = endIndex + 1;
            }
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed @ " + position + "/" + startIndex + ": " + action, e);
        }
    }

    @Override
    public long startIndex() {
        return startIndex;
    }

    @Override
    public long endIndex() {
        return endIndex;
    }

    @Override
    public MemorySegment memorySegment() {
        return segment;
    }

    @Override
    public String toString() {
        String segmentString = LineSegments.toString(this);
        return getClass().getSimpleName() + "[" + partition + " " + segmentString + "]";
    }
}
