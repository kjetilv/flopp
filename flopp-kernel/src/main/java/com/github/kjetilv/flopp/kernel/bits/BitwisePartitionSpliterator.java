package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.ImmutableLine;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

public final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Mediator<LineSegment> mediator;

    private final BitwisePartitionSpliterator next;

    private final MutableLine line = new MutableLine();

    private final MemorySegment segment;

    private final long limit;

    private final long physicalLimit;

    /**
     * Current mask
     */
    private long mask;

    /**
     * Current offset into partition, aligned to long size (8 bytes)
     */
    private long alignedOffset;

    /**
     * Position of line in progress
     */
    private long lineStart;

    public BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        Mediator<LineSegment> mediator,
        BitwisePartitionSpliterator next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.line.memorySegment = this.segment = segment;

        this.limit = this.partition.length();
        this.physicalLimit = this.segment.byteSize();
        this.mediator = mediator;
        this.next = next;
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            if (!partition.first()) {
                lineStart = findFirstLine();
                if (lineStart == limit) {
                    return false;
                }
            }
            Consumer<LineSegment> consumer = mediate(action);
            if (partition.last()) {
                processTail(consumer);
            } else {
                processAligned(consumer);
            }
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[@\{alignedOffset}/l:\{lineStart} \{partition}]";
    }

    private BitwisePartitionSpliterator duplicate() {
        return new BitwisePartitionSpliterator(partition, segment, mediator, next);
    }

    private long findFirstLine() {
        do {
            mask = mask(loadLong());
            if (mask != 0) {
                long start = alignedOffset + offsetIn(mask) + 1;
                mask &= CLEARED[Math.toIntExact(start - alignedOffset)];
                if (mask == 0) {
                    alignedOffset += ALIGNMENT;
                }
                return start;
            }
            alignedOffset += ALIGNMENT;
        } while (alignedOffset < limit);
        return limit;
    }

    private void processAligned(Consumer<LineSegment> action) {
        if (mask != 0) {
            while (mask != 0) {
                shipLine(action);
            }
            alignedOffset += ALIGNMENT;
        }
        while (alignedOffset < limit) {
            mask = mask(loadLong());
            while (mask != 0) {
                shipLine(action);
            }
            alignedOffset += ALIGNMENT;
        }
        while (alignedOffset < physicalLimit) {
            mask = mask(loadLong());
            if (mask != 0) {
                shipLine(action);
                return;
            }
            alignedOffset += ALIGNMENT;
        }
        action.accept(collectedLastLine());
    }

    private void processTail(Consumer<LineSegment> action) {
        if (mask != 0) {
            while (mask != 0) {
                shipLine(action);
            }
            alignedOffset += ALIGNMENT;
        }
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        while (alignedOffset < lastOffset) {
            mask = mask(loadLong());
            while (mask != 0) {
                shipLine(action);
            }
            alignedOffset += ALIGNMENT;
        }
        if (tail > 0) {
            mask = mask(loadTail(tail));
            if (mask != 0) {
                while (mask != 0) {
                    shipLine(action);
                }
            } else {
                ship(action, physicalLimit - lineStart);
            }
        }
    }

    private LineSegment collectedLastLine() {
        long nextOffset = next.duplicate().findFirstLine();
        if (nextOffset < next.limit) {
            return merge(next.segment, nextOffset - 1);
        }
        if (next.partition.last()) {
            return merge(next.segment, nextOffset);
        }
        List<BitwisePartitionSpliterator> collector = new ArrayList<>();
        long lastLimit = collectAndFindLimit(collector);
        return combineMultiple(collector, lastLimit);
    }

    @SuppressWarnings("unchecked")
    private Consumer<LineSegment> mediate(Consumer<? super LineSegment> action) {
        return (Consumer<LineSegment>) (mediator == null ? action : mediator.apply(action));
    }

    private void shipLine(Consumer<LineSegment> action) {
        long lineBreakOffset = alignedOffset + offsetIn(mask);
        ship(action, lineBreakOffset - lineStart);
        lineStart = lineBreakOffset + 1;
        mask &= CLEARED[Math.toIntExact(lineStart - alignedOffset)];
    }

    private void ship(Consumer<LineSegment> action, long length) {
        line.offset = lineStart;
        line.length = length;
        action.accept(line);
    }

    private LineSegment merge(MemorySegment next, long preamble) {
        long trail = limit - lineStart;
        MemorySegment segment = MemorySegment.ofArray(new byte[Math.toIntExact(trail + preamble)]);
        MemorySegment.copy(this.segment, lineStart, segment, 0, trail);
        MemorySegment.copy(next, 0, segment, trail, preamble);
        return new ImmutableLine(segment);
    }

    private long collectAndFindLimit(List<BitwisePartitionSpliterator> collector) {
        BitwisePartitionSpliterator spliterator = next;
        collector.add(spliterator);
        while (true) {
            BitwisePartitionSpliterator nextSpliterator = spliterator.next;
            if (nextSpliterator == null) {
                return spliterator.limit;
            }
            spliterator = nextSpliterator;
            collector.add(spliterator);
            long lo = 0L;
            while (lo < spliterator.physicalLimit) {
                long prebyte = bytesAt(spliterator.segment, lo);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + offsetIn(premask);
                }
                lo += ALIGNMENT;
            }
        }
    }

    private LineSegment combineMultiple(
        List<BitwisePartitionSpliterator> collector,
        long lastPreamble
    ) {
        long trail = limit - lineStart;
        int mediaries = collector.size() - 1;
        long mediarySize = collector.stream()
            .limit(mediaries)
            .mapToLong(spliterator -> spliterator.limit)
            .sum();
        MemorySegment segment = MemorySegment.ofArray(new byte[Math.toIntExact(trail + mediarySize + lastPreamble)]);
        MemorySegment.copy(this.segment, lineStart, segment, 0, trail);
        long accumulatedSize = trail;
        for (int i = 0; i < mediaries; i++) {
            BitwisePartitionSpliterator step = collector.get(i);
            MemorySegment.copy(step.segment, 0, segment, accumulatedSize, step.limit);
            accumulatedSize += step.limit;
        }
        MemorySegment.copy(collector.getLast().segment, 0, segment, accumulatedSize, lastPreamble);
        return new ImmutableLine(segment);
    }

    private long loadLong() {
        return bytesAt(segment, alignedOffset);
    }

    private long loadTail(long count) {
        long l = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = byteAt(segment, alignedOffset + i);
            l = (l << ALIGNMENT) + b;
        }
        return l;
    }

    public static final long ALIGNMENT = JAVA_LONG.byteAlignment();

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

    private static byte byteAt(MemorySegment segment, long offset) {
        return segment.get(ValueLayout.JAVA_BYTE, offset);
    }

    private static long bytesAt(MemorySegment segement, long index) {
        return segement.get(JAVA_LONG, index);
    }

    private static long mask(long bytes) {
        long masked = bytes ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static long offsetIn(long mask) {
        return Long.numberOfTrailingZeros(mask) / ALIGNMENT;
    }
}
