package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

public final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Mediator<LineSegment> mediator;

    private final BitwisePartitionSpliterator next;

    private final MutableLine line = new MutableLine();

    private final MemorySegment memorySegment;

    /**
     * Current mask
     */
    private long mask;

    /**
     * Current offset into partition, aligned to long size (8 bytes)
     */
    private long alignedOffset = -ALIGNMENT;

    /**
     * Current offset into partition, by byte
     */
    private long offset;

    /**
     * Position of line in progress
     */
    private long lineStart;

    public BitwisePartitionSpliterator(
        Partition partition,
        MemorySegmentSource memorySegmentSource,
        Mediator<LineSegment> mediator,
        BitwisePartitionSpliterator next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.mediator = mediator;
        this.next = partition.last() ? null : Objects.requireNonNull(next, "next");

        this.line.memorySegment = memorySegment = memorySegmentSource.open(partition);
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
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
        return STR."\{getClass().getSimpleName()}[offset:\{offset} \{partition}]";
    }

    @SuppressWarnings("unchecked")
    private Consumer<LineSegment> mediate(Consumer<? super LineSegment> action) {
        return (Consumer<LineSegment>) (
            mediator == null
                ? action
                : mediator.apply(action)
        );
    }

    private void skipToStart() {
        do {
            loadLong();
        } while (mask == 0);
        progressMask();
        clear();
        lineStart = offset;
    }

    private void processAligned(Consumer<LineSegment> action, long limit) {
        while (offset <= limit) {
            while (mask == 0) {
//                { // loadLong unrolled
                advanceOffsets();
                long l = memorySegment.get(JAVA_LONG, alignedOffset);
                mask = mask(l);
//                }
            }
            while (mask != 0) { // feedLines unrolled
//                { // progressMask unrolled
                long leap = Long.numberOfTrailingZeros(mask) / ALIGNMENT;
                offset = alignedOffset + leap;
//                }
//                { // feedLine runrolled
                long shift = offset - lineStart;

                line.offset = lineStart;
                line.length = shift;
                lineStart += shift + 1;

                action.accept(line);
//                }
//                { // clear unrolled
                offset++;
                mask &= CLEARED[Math.toIntExact(offset - alignedOffset)];
//                }
            }
        }
    }

    private void processTail(Consumer<LineSegment> action, long limit) {
        long tail = partition.last()
            ? partition.count() % partition.alignment()
            : 0L;
        long lastLoadableOffset = limit - tail - ALIGNMENT;
        while (offset < limit) {
            while (mask == 0 && offset < lastLoadableOffset) {
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

    private void feedLine(Consumer<LineSegment> action) {
        long shift = offset - lineStart;

        line.offset = lineStart;
        line.length = shift;
        lineStart += shift + 1;

        action.accept(line);
    }

    private void loadLong() {
        advanceOffsets();
        long l = memorySegment.get(JAVA_LONG, alignedOffset);
        mask = mask(l);
    }

    private void loadTail(long tail) {
        advanceOffsets();
        long l = loadBytes(tail);
        mask = mask(l);
    }

    private void progressMask() {
        offset = alignedOffset + Long.numberOfTrailingZeros(mask) / ALIGNMENT;
    }

    private long loadBytes(long count) {
        long l = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(ValueLayout.JAVA_BYTE, alignedOffset + i);
            l = (l << ALIGNMENT) + b;
        }
        return l;
    }

    private void advanceOffsets() {
        alignedOffset += ALIGNMENT;
        offset = alignedOffset;
    }

    private void clear() {
        offset++;
        mask &= CLEARED[Math.toIntExact(offset - alignedOffset)];
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

    public static final long ALIGNMENT = JAVA_LONG.byteAlignment();

    private static long mask(long l) {
        long masked = l ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }
}
