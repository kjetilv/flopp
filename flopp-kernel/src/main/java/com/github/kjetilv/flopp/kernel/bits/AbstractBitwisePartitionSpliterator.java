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

    private int currentOffset;

    private long longOffset;

    private long byteOffset;

    private long lineStart;

    private long mask;

    protected AbstractBitwisePartitionSpliterator(Partition partition, MemorySegment memorySegment) {
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
    public String toString() {
        String stringAddendum = toStringAddendum();
        String name = getClass().getSimpleName();
        return stringAddendum == null || stringAddendum.isBlank()
            ? STR."\{name}[offset:\{byteOffset} \{partition}]"
            : STR."\{name}[offset:\{byteOffset} \{partition} \{stringAddendum.trim()}]";
    }

    protected String toStringAddendum() {
        return "";
    }

    protected final void processAligned(Consumer<? super LineSegment> action) {
        while (byteOffset <= limit) {
            do {
                loadLong();
            } while (mask == 0);
            do {
                progressMask();
                shipLine(action);
                clearLn();
            } while (mask != 0);
        }
    }

    protected final void processTail(Consumer<? super LineSegment> action, int tail) {
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
                clearLn();
            } while (mask != 0);
        }
    }

    protected final void skipToStart() {
        do {
            loadLong();
        } while (mask == 0);
        progressMask();
        clearLn();
        lineStart = byteOffset;
    }

    private void shipLine(Consumer<? super LineSegment> action) {
        long shift = byteOffset - lineStart;

        line.offset = lineStart;
        line.length = shift - 1;
        action.accept(line);

        lineStart += shift;
    }

    private void clearLn() {
        mask &= ALL_CLEAR[currentOffset];
    }

    private void progressMask() {
        int leap = trailingBytes(mask);
        byteOffset += leap - currentOffset;
        currentOffset = leap;
    }

    private void loadLong() {
        newLong(memorySegment.get(ValueLayout.JAVA_LONG, longOffset));
        longOffset += alignment;
        mask = mask(current);
    }

    private void newLong(long l) {
        current = l;
        currentOffset = 0;
        byteOffset = longOffset;
    }

    private void loadTail(int tail) {
        newLong(0L);
        for (int i = tail - 1; i >= 0; i--) {
            byte b = memorySegment.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            current = (current << alignment) + b;
        }
        mask = mask(current);
    }

    private static final long[] ALL_CLEAR = {
        0x8080808080808080L,
        0x8080808080808000L,
        0x8080808080800000L,
        0x8080808080000000L,
        0x8080808000000000L,
        0x8080800000000000L,
        0x8080000000000000L,
        0x8000000000000000L,
        0x0000000000000000L,
    };

    private static long mask(long l) {
        long masked = l ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static int trailingBytes(long mask) {
        return Long.numberOfTrailingZeros(mask) / 8 + 1;
    }
}
