package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class BitwisePartitionSpliterator2 extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final MemorySegment memorySegment;

    private final long limit;

    private final int alignment;

    private final MutableLine line;

    private final int tail;

    private long current;

    private int currentOffset;

    private long longOffset;

    private long byteOffset;

    private long lineStart;

    private long mask;

    BitwisePartitionSpliterator2(Partition partition, MemorySegment memorySegment) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        this.memorySegment = Objects.requireNonNull(memorySegment, "memorySegmentSources");
        this.partition = Objects.requireNonNull(partition, "partition");

        this.alignment = this.partition.alignment();
        this.limit = partition.count();

        this.line = new MutableLine();
        this.line.partitionNo = partition.partitionNo();
        this.line.memorySegment = memorySegment;

        this.tail = partition.last() ? Math.toIntExact(partition.count() % partition.alignment()) : 0;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        if (partition.first()) {
            processAligned(action);
            return false;
        }
        skipToStart();
        if (partition.last()) {
            processTail(action, tail);
            return false;
        }
        processAligned(action);
        return false;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[offset:\{byteOffset} \{partition}]";
    }

    private void processAligned(Consumer<? super LineSegment> action) {
        while (byteOffset <= limit) {
            do {
                loadLong();
            } while (mask == 0);
            drainTo(action);
        }
    }

    private void processTail(Consumer<? super LineSegment> action, int tail) {
        long lastLongOffset = limit - tail - alignment;
        while (byteOffset < limit) {
            while (mask == 0 && byteOffset < lastLongOffset) {
                loadLong();
            }
            if (mask == 0 && tail > 0) {
                loadTail();
            }
            drainTo(action);
        }
    }

    private void drainTo(Consumer<? super LineSegment> action) {
        do {
            progressMask();
            shipLine(action);
            clearLn();
        } while (mask != 0);
    }

    private void skipToStart() {
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
        mask &= CLEARED[currentOffset];
    }

    private void progressMask() {
        int leap = Long.numberOfTrailingZeros(mask) / 8 + 1;
        byteOffset += leap - currentOffset;
        currentOffset = leap;
    }

    private void loadLong() {
        newLong(memorySegment.get(ValueLayout.JAVA_LONG, longOffset));
        longOffset += alignment;
        mask = mask();
    }

    private void loadTail() {
        newLong(0L);
        for (int i = tail - 1; i >= 0; i--) {
            byte b = memorySegment.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            current = (current << alignment) + b;
        }
        mask = mask();
    }

    private void newLong(long l) {
        current = l;
        currentOffset = 0;
        byteOffset = longOffset;
    }

    private long mask() {
        long masked = current ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static final long[] CLEARED = {
        0xFFFFFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFF00L,
        0xFFFFFFFFFFFF0000L,
        0xFFFFFFFFFF000000L,
        0xFFFFFFFF00000000L,
        0xFFFFFF0000000000L,
        0xFFFF000000000000L,
        0xFF00000000000000L,
        0x0000000000000000L,
    };
}
