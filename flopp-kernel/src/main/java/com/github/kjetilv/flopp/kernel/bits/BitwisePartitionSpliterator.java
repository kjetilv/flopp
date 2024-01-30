package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final MemorySegmentSource memorySegmentSource;

    private final Mediator<LineSegment> mediator;

    private final int alignment;

    private final MutableLine line;

    /**
     * Current mask
     */
    private long mask;

    /**
     * Current offset into partition, aligned to long size (8 bytes)
     */
    private long alignedOffset;

    /**
     * Current offset into partition, by byte
     */
    private long byteOffset;

    /**
     * Position of line in progress
     */
    private long lineStart;

    public BitwisePartitionSpliterator(
        Partition partition,
        MemorySegmentSource memorySegmentSource,
        Mediator<LineSegment> mediator
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
        this.mediator = mediator;
        this.alignment = this.partition.alignment();

        this.alignedOffset = -alignment;
        this.line = new MutableLine();
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            line.memorySegment = memorySegmentSource.open(partition);
            if (!partition.first()) {
                skipToStart();
            }
            long limit = partition.count();
            Consumer<LineSegment> consumer = mediate(action);
            if (!partition.last()) {
                processAligned(consumer, limit);
            } else {
                processTail(consumer, limit);
            }
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[offset:\{byteOffset} \{partition}]";
    }

    @SuppressWarnings("unchecked")
    private Consumer<LineSegment> mediate(Consumer<? super LineSegment> action) {
        return (Consumer<LineSegment>) (
            mediator == null
                ? action
                : mediator.apply(action)
        );
    }

    private void processAligned(Consumer<LineSegment> action, long limit) {
        while (byteOffset <= limit) {
            while (mask == 0) {
                loadLong();
            }
            feedLines(action);
        }
    }

    private void processTail(Consumer<LineSegment> action, long limit) {
        long tail = partition.last()
            ? partition.count() % partition.alignment()
            : 0L;
        long lastLoadableOffset = limit - tail - alignment;
        while (byteOffset < limit) {
            while (mask == 0 && byteOffset < lastLoadableOffset) {
                loadLong();
            }
            if (mask == 0 && tail > 0) {
                loadTail(tail);
            }
            feedLines(action);
        }
    }

    private void feedLines(Consumer<LineSegment> action) {
        while (mask != 0) {
            progressMask();
            feedLine(action);
            clear();
        }
    }

    private void skipToStart() {
        do {
            loadLong();
        } while (mask == 0);
        progressMask();
        clear();
        lineStart = byteOffset;
    }

    private void feedLine(Consumer<LineSegment> action) {
        long shift = byteOffset - lineStart;

        line.offset = lineStart;
        line.length = shift - 1;
        lineStart += shift;

        action.accept(line);
    }

    private void progressMask() {
        int leap = Long.numberOfTrailingZeros(mask) / BYTES_IN_LONG + 1;
        byteOffset = alignedOffset + leap;
    }

    private void clear() {
        mask &= CLEARED[Math.toIntExact(byteOffset - alignedOffset)];
    }

    private void loadLong() {
        byteOffset = alignedOffset += alignment;
        long l = line.memorySegment.get(ValueLayout.JAVA_LONG, alignedOffset);
        mask = mask(l);
    }

    private void loadTail(long tail) {
        byteOffset = alignedOffset += alignment;
        long l = loadBytes(tail);
        mask = mask(l);
    }

    private long loadBytes(long count) {
        long l = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = line.memorySegment.get(ValueLayout.JAVA_BYTE, alignedOffset + i);
            l = (l << alignment) + b;
        }
        return l;
    }

    private static final int BYTES_IN_LONG = 8;

    private static long mask(long l) {
        long masked = l ^ 0x0A0A0A0A0A0A0A0AL;
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
        0x0000000000000000L
    };
}
