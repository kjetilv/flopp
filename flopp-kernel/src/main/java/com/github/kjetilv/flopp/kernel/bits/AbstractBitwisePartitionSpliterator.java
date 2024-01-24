package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

@SuppressWarnings("ProtectedField")
abstract sealed class AbstractBitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<LineSegment>
    permits BitwiseLineSeekPartitionSpliterator,
    BitwiseInitialPartitionSpliterator,
    BitwiseTrailingPartitionSpliterator {

    protected final Partition partition;

    protected final MutableLine l;

    protected MemorySegment ms;

    protected long current;

    protected long longOffset;

    protected long byteOffset;

    protected long currentLineStart;

    protected long currentMask;

    private int leap;

    private final int alignment;

    private final long limit;

    AbstractBitwisePartitionSpliterator(Partition partition, MemorySegment ms) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.ms = Objects.requireNonNull(ms, "memorySegmentSources");

        this.partition = Objects.requireNonNull(partition, "partition");
        this.alignment = this.partition.alignment();
        this.limit = partition.count();

        this.l = new MutableLine();
        this.l.partitionNo = partition.partitionNo();
        this.l.memorySegment = ms;
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
        long length = byteOffset - currentLineStart - 1;

        l.offset = currentLineStart;
        l.length = length;
        action.accept(l);
        currentLineStart += length + 1;
    }

    protected void clearLeap() {
        if (leap == alignment - 1) {
            currentMask = 0;
        } else {
            currentMask >>>= (leap + 1) * alignment;
        }
    }

    protected void progressMask() {
        leap = Bits.trailingBytes(currentMask);
        byteOffset += leap + 1;
    }

    protected final void loadLong() {
        try {
            current = ms.get(ValueLayout.JAVA_LONG, longOffset);
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to load from \{longOffset} in \{ms}", e);
        }
        byteOffset = longOffset;
        longOffset += alignment;
        currentMask = Bits.currentLongMask(current);
    }

    protected void skipToStart() {
        do {
            loadLong();
        } while (currentMask == 0);
        partitionStarted();
    }

    protected void partitionStarted() {
        progressMask();
        clearLeap();
        currentLineStart = byteOffset;
    }

    protected void processAligned(Consumer<? super LineSegment> action) {
        while (byteOffset <= limit) {
            do {
                loadLong();
            } while (currentMask == 0);
            do {
                progressMask();
                shipLine(action);
                clearLeap();
            } while (currentMask != 0);
        }
    }

    protected void processTail(Consumer<? super LineSegment> action, int tail) {
        long lastLongOffset = limit - tail - alignment;
        while (byteOffset < limit) {
            while (currentMask == 0 && byteOffset < lastLongOffset) {
                loadLong();
            }
            if (currentMask == 0 && tail > 0) {
                loadTail(tail);
            }
            do {
                progressMask();
                shipLine(action);
                clearLeap();
            } while (currentMask != 0);
        }
    }

    protected void loadTail(int tail) {
        current = 0L;
        byteOffset = longOffset;
        for (int i = tail - 1; i >= 0; i--) {
            byte b = ms.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            current = (current << alignment) + b;
        }
        currentMask = Bits.currentLongMask(current);
    }
}
