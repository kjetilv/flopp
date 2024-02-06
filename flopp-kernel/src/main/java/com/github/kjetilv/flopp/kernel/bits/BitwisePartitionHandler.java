package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class BitwisePartitionHandler
    implements Runnable {

    private final Partition partition;

    private final Supplier<BitwisePartitionHandler> next;

    private final MemorySegment segment;

    private final Action action;

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

    BitwisePartitionHandler(
        Partition partition,
        MemorySegment segment,
        Action action,
        Supplier<BitwisePartitionHandler> next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = Objects.requireNonNull(segment, "segment");
        this.action = Objects.requireNonNull(action, "action");
        this.next = next;

        this.limit = this.partition.length();
        this.physicalLimit = this.segment.byteSize();
        if (limit > physicalLimit) {
            throw new IllegalStateException(STR."\{this} got bad segment \{segment}");
        }
    }

    public void run() {
        try (action) {
            if (!partition.first()) {
                lineStart = findFirstLine();
                if (lineStart == limit) {
                    return;
                }
                processHead();
            }
            if (partition.last()) {
                processTail();
            } else {
                processBody();
            }
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[@\{alignedOffset}/l:\{lineStart} in \{partition}]";
    }

    private BitwisePartitionHandler duplicate() {
        return new BitwisePartitionHandler(partition, segment, action, next);
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

    private void processHead() {
        if (mask == 0) {
            return;
        }
        while (mask != 0) {
            shipNextLine();
        }
        alignedOffset += ALIGNMENT;
    }

    private void processBody() {
        processMain(limit);
        if (processedOverflow()) {
            return;
        }
        stealWork();
    }

    private void processTail() {
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        processMain(lastOffset);
        if (tail > 0) {
            mask = mask(loadTail(tail));
            if (mask != 0) {
                while (mask != 0) {
                    shipNextLine();
                }
            } else {
                action.line(segment, lineStart, physicalLimit);
            }
        }
    }

    private void processMain(long lastOffset) {
        while (alignedOffset < lastOffset) {
            mask = mask(loadLong());
            while (mask != 0) {
                shipNextLine();
            }
            alignedOffset += ALIGNMENT;
        }
    }

    private boolean processedOverflow() {
        while (alignedOffset < physicalLimit) {
            mask = mask(loadLong());
            if (mask != 0) {
                shipNextLine();
                return true;
            }
            alignedOffset += ALIGNMENT;
        }
        return false;
    }

    private void shipNextLine() {
        int offsetInMask = offsetIn(mask);
        long lineBreakOffset = alignedOffset + offsetInMask;
        action.line(segment, lineStart, lineBreakOffset);
        lineStart = lineBreakOffset + 1;
        mask &= CLEARED[offsetInMask + 1];
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

    private void stealWork() {
        BitwisePartitionHandler next = this.next.get();
        long nextOffset = next.duplicate().findFirstLine();
        if (nextOffset < next.limit) {
            mergeWithNext(next, nextOffset - 1);
        } else if (next.partition.last()) {
            mergeWithNext(next, nextOffset);
        } else {
            mergeWithMultiple(next);
        }
    }

    private void mergeWithNext(BitwisePartitionHandler next, long preamble) {
        long trail = limit - lineStart;
        int length = Math.toIntExact(trail + preamble);
        MemorySegment buffer = MemorySegment.ofArray(new byte[length]);
        MemorySegment.copy(this.segment, lineStart, buffer, 0, trail);
        MemorySegment.copy(next.segment, 0, buffer, trail, preamble);
        action.line(buffer, 0, length);
    }

    private void mergeWithMultiple(BitwisePartitionHandler next) {
        List<BitwisePartitionHandler> collector = new ArrayList<>();
        long lastLimit = collectAndFindLimit(next, collector);
        combineMultiple(collector, lastLimit);
    }

    private void combineMultiple(List<BitwisePartitionHandler> collector, long lastLineOffset) {
        long trail = limit - lineStart;
        int mediaries = collector.size() - 1;
        long mediarySize = collector.stream()
            .limit(mediaries)
            .mapToLong(spliterator -> spliterator.limit)
            .sum();
        int length = Math.toIntExact(trail + mediarySize + lastLineOffset);
        MemorySegment buffer = MemorySegment.ofArray(new byte[length]);
        MemorySegment.copy(segment, lineStart, buffer, 0, trail);
        long accumulatedSize = trail;
        for (int i = 0; i < mediaries; i++) {
            BitwisePartitionHandler step = collector.get(i);
            MemorySegment.copy(
                step.segment,
                0,
                buffer,
                accumulatedSize,
                step.limit
            );
            accumulatedSize += step.limit;
        }
        MemorySegment.copy(
            collector.getLast().segment,
            0,
            buffer,
            accumulatedSize,
            lastLineOffset
        );
        action.line(buffer, 0, length);
    }

    private static final int ALIGNMENT = 0x08;

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

    private static int offsetIn(long mask) {
        return Long.numberOfTrailingZeros(mask) / ALIGNMENT;
    }

    private static long collectAndFindLimit(
        BitwisePartitionHandler resolvedNext,
        List<BitwisePartitionHandler> collector
    ) {
        BitwisePartitionHandler next = resolvedNext;
        collector.add(next);
        while (true) {
            BitwisePartitionHandler nextNext = next.next.get();
            if (nextNext == null) {
                return next.limit;
            }
            next = nextNext;
            collector.add(next);
            long lo = 0L;
            while (lo < next.physicalLimit) {
                long prebyte = bytesAt(next.segment, lo);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + offsetIn(premask);
                }
                lo += ALIGNMENT;
            }
        }
    }

    @FunctionalInterface
    public interface Action extends Closeable {

        void line(MemorySegment segment, long startIndex, long endIndex);

        @Override
        default void close() {
        }
    }

    @FunctionalInterface
    public interface Mediator extends Function<Action, Action> {
    }
}
