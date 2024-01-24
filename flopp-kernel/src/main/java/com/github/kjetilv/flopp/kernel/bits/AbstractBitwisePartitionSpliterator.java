package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

abstract sealed class AbstractBitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<LineSegment>
    permits BitwiseLineSeekPartitionSpliterator,
    BitwiseInitialPartitionSpliterator,
    BitwiseTrailingPartitionSpliterator {

    private final Partition partition;

    private final MemorySegment memorySegment;

    private final long limit;

    private final int alignment;

    private final MutableLine line;

    private long current;

    private long longOffset;

    private long byteOffset;

    private long lineStart;

    private long mask;

    private int leap;

    AbstractBitwisePartitionSpliterator(Partition partition, MemorySegment memorySegment) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.memorySegment = Objects.requireNonNull(memorySegment, "memorySegmentSources");

        this.partition = Objects.requireNonNull(partition, "partition");
        this.alignment = this.partition.alignment();
        this.limit = partition.count();

        this.line = new MutableLine();
        this.line.partitionNo = partition.partitionNo();
        this.line.memorySegment = memorySegment;
    }

    @Override
    public final boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            return advance(action);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to process", e);
        }
    }

    @Override
    public String toString() {
        String stringAddendum = toStringAddendum();
        String name = getClass().getSimpleName();
        return stringAddendum == null || stringAddendum.isBlank()
            ? STR."\{name}[offset:\{byteOffset} \{partition}]"
            : STR."\{name}[offset:\{byteOffset} \{partition} \{stringAddendum.trim()}]";
    }

    abstract boolean advance(Consumer<? super LineSegment> action);

    protected String toStringAddendum() {
        return "";
    }

    protected final void shipLine(Consumer<? super LineSegment> action) {
        long shift = byteOffset - lineStart;
        lineStart += shift;

        line.offset = lineStart;
        line.length = shift - 1;
        action.accept(this.line);
    }

    protected void clearLeap() {
        if (leap == alignment) {
            mask = 0;
        } else {
            mask >>>= leap * alignment;
        }
    }

    protected void progressMask() {
        byteOffset += leap = Bits.trailingBytes(mask) + 1;
    }

    protected final void loadLong() {
        try {
            current = memorySegment.get(ValueLayout.JAVA_LONG, longOffset);
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to load from \{longOffset} in \{memorySegment}", e);
        }
        byteOffset = longOffset;
        longOffset += alignment;
        mask = Bits.mask(current);
    }

    protected void skipToStart() {
        do {
            loadLong();
        } while (mask == 0);
        partitionStarted();
    }

    protected void partitionStarted() {
        progressMask();
        clearLeap();
        lineStart = byteOffset;
    }

    protected void processAligned(Consumer<? super LineSegment> action) {
        while (byteOffset <= limit) {
            do {
                loadLong();
            } while (mask == 0);
            do {
                progressMask();
                shipLine(action);
                clearLeap();
            } while (mask != 0);
        }
    }

    protected void processTail(Consumer<? super LineSegment> action, int tail) {
        long lastLongOffset = limit - tail - alignment;
        while (byteOffset < limit) {
            while (mask == 0 && byteOffset < lastLongOffset) {
                loadLong();
            }
            if (mask == 0 && tail > 0) {
                loadTail(tail);
            }
            do {
                progressMask();
                shipLine(action);
                clearLeap();
            } while (mask != 0);
        }
    }

    protected void loadTail(int tail) {
        current = 0L;
        byteOffset = longOffset;
        for (int i = tail - 1; i >= 0; i--) {
            byte b = memorySegment.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            current = (current << alignment) + b;
        }
        mask = Bits.mask(current);
    }
}
