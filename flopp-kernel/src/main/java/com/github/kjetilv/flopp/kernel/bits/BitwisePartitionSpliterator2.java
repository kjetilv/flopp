package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class BitwisePartitionSpliterator2 extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Mediator<LineSegment> mediator;

    private final MemorySegment memorySegment;

    private final int alignment;

    private final MutableLine line;

    private long currentLong;

    private int currentLongOffset;

    private long longAlignedOffset;

    private long offset;

    private long lineStart;

    private long mask;

    BitwisePartitionSpliterator2(Partition partition, MemorySegment memorySegment) {
        this(partition, memorySegment, null);
    }

    BitwisePartitionSpliterator2(
        Partition partition,
        MemorySegment memorySegment,
        Mediator<LineSegment> mediator
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        this.memorySegment = Objects.requireNonNull(memorySegment, "memorySegmentSources");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.mediator = mediator;

        this.alignment = this.partition.alignment();

        this.line = new MutableLine();
        this.line.partitionNo = partition.partitionNo();
        this.line.memorySegment = this.memorySegment;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        Consumer<LineSegment> consumer = (Consumer<LineSegment>) (mediator == null ? action : mediator.apply(action));
        if (partition.first()) {
            processAligned(consumer);
            return false;
        }
        skipToStart();
        if (partition.last()) {
            processTail(consumer);
            return false;
        }
        processAligned(consumer);
        return false;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[offset:\{offset} \{partition}]";
    }

    private void processAligned(Consumer<LineSegment> action) {
        long partitionLimit = partition.count();
        while (offset <= partitionLimit) {
            do {
                loadLong();
            } while (mask == 0);
            drainTo(action);
        }
    }

    private void processTail(Consumer<LineSegment> action) {
        long partitionLimit = partition.count();
        long tail = partition.last() ? partition.count() % partition.alignment() : 0L;
        long lastLongOffset = partitionLimit - tail - alignment;
        while (offset < partitionLimit) {
            while (mask == 0 && offset < lastLongOffset) {
                loadLong();
            }
            if (mask == 0 && tail > 0) {
                loadTail(tail);
            }
            drainTo(action);
        }
    }

    private void drainTo(Consumer<LineSegment> action) {
        do {
            progressMask();
            shipLine(action);
            clear();
        } while (mask != 0);
    }

    private void skipToStart() {
        do {
            loadLong();
        } while (mask == 0);
        progressMask();
        clear();
        lineStart = offset;
    }

    private void shipLine(Consumer<LineSegment> action) {
        long shift = offset - lineStart;

        line.offset = lineStart;
        line.length = shift - 1;
        action.accept(line);

        lineStart += shift;
    }

    private void progressMask() {
        int leap = Long.numberOfTrailingZeros(mask) / BYTES_IN_LONG + 1;
        offset += leap - currentLongOffset;
        currentLongOffset = leap;
    }

    private void clear() {
        mask &= CLEARED[currentLongOffset];
    }

    private void loadLong() {
        set(memorySegment.get(ValueLayout.JAVA_LONG, longAlignedOffset));
        longAlignedOffset += alignment;
        mask();
    }

    private void loadTail(long tail) {
        set(0L);
        loadBytes(tail);
        mask();
    }

    private void loadBytes(long count) {
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(ValueLayout.JAVA_BYTE, offset + i);
            currentLong = (currentLong << alignment) + b;
        }
    }

    private void set(long l) {
        currentLong = l;
        currentLongOffset = 0;
        offset = longAlignedOffset;
    }

    private void mask() {
        long masked = currentLong ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        mask = clearedHighBits & 0x8080808080808080L;
    }

    private static final int BYTES_IN_LONG = 8;

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
