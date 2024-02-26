package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT_INT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.of;
import static java.lang.foreign.MemorySegment.copy;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class BitwisePartitionHandler implements Runnable {

    private final Partition partition;

    private final Supplier<BitwisePartitionHandler> next;

    private final MemorySegment segment;

    private final BitwisePartitioned.Action action;

    private final long limit;

    private final long physicalLimit;

    private long lineStart;

    private long offset;

    BitwisePartitionHandler(
        Partition partition,
        MemorySegment segment,
        BitwisePartitioned.Action action,
        Supplier<BitwisePartitionHandler> next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = Objects.requireNonNull(segment, "segment");
        this.action = Objects.requireNonNull(action, "action");
        this.next = next;

        this.limit = this.partition.length();
        this.physicalLimit = this.segment.byteSize();
        if (this.limit > this.physicalLimit) {
            throw new IllegalStateException(STR."\{this} got bad segment \{segment}");
        }
    }

    public void run() {
        try (action) {
            if (processHead()) {
                if (partition.last()) {
                    processTail();
                } else {
                    processBody();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed @ \{offset}/\{lineStart}: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    private boolean processHead() {
        if (partition.first()) {
            return true;
        }
        Long init = initialize();
        if (init == null) {
            return false; // No newlines in this partition
        }
        long mask = init;
        if (mask != 0) {
            do { // Newlines found in current mask
                mask = shipNextLine(mask);
            } while (mask != 0);
            offset += ALIGNMENT;
        }
        return true;
    }

    private void processBody() {
        processMain(limit);
        if (!processedOverflow()) {
            transcend(this.next.get()); // We need to query the next partition
        }
    }

    private Long initialize() {
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        while (offset < lastOffset) {
            long bytes = loadLong();
            long mask = mask(bytes);
            if (mask != 0) {
                return initializeFrom(mask);
            }
            offset += ALIGNMENT;
        }
        if (tail > 0) {
            long bytes = loadTail(tail);
            long mask = mask(bytes);
            if (mask != 0) {
                return initializeFrom(mask);
            }
            offset += ALIGNMENT;
        }
        return null; // No newline found in the whole partition
    }

    private void processTail() {
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        processMain(lastOffset);
        if (tail > 0) {
            processTail(tail);
        }
        if (lineStart < physicalLimit) {
            emitAndAdvance(physicalLimit);
        }
    }

    private void processMain(long lastOffset) {
        while (offset < lastOffset) {
            long bytes = loadLong();
            long mask = mask(bytes);
            while (mask != 0) {
                mask = shipNextLine(mask);
            }
            offset += ALIGNMENT;
        }
    }

    private boolean processedOverflow() {
        long tail = physicalLimit % ALIGNMENT;
        long lastAligned = physicalLimit - tail;
        while (offset < lastAligned) {
            long bytes = loadLong();
            long mask = mask(bytes);
            if (mask == 0) {
                offset += ALIGNMENT;
            } else {
                shipNextLine(mask);
                return true;
            }
        }
        if (tail > 0) {
            long mask = mask(loadTail(tail));
            if (mask != 0) { // Tail did not end in newline, send what we got
                shipNextLine(mask);
                return true;
            }
        }
        return false;
    }

    private void processTail(long tail) {
        long mask = mask(loadTail(tail));
        if (mask == 0) { // Tail did not end in newline, send what we got
            emitAndAdvance(physicalLimit);
        } else {
            do { // Newlines spotted, ship lines
                mask = shipNextLine(mask);
            } while (mask != 0);
            if (lineStart < physicalLimit) { // Tail did not end in newline, send what we got
                emitAndAdvance(physicalLimit);
            }
        }
    }

    private long findFirstLine() {
        long offset = 0L;
        while (offset < limit) {
            long mask = mask(segment.get(JAVA_LONG, offset));
            if (mask != 0) {
                return offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT + 1;
            }
            offset += ALIGNMENT;
        }
        return limit;
    }

    private Long initializeFrom(long bytes) {
        long mask = bytes;
        long start = offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT + 1;
        if (start == physicalLimit) {
            return null; // First linebreak was the last
        }
        mask &= CLEARED[Math.toIntExact(start - offset)];
        if (mask == 0) { // We cleared the current mask
            offset += ALIGNMENT;
        }
        // Mark position of new line
        lineStart = start;
        return mask;
    }

    private long shipNextLine(long mask) {
        int offsetInMask = Long.numberOfTrailingZeros(mask) / ALIGNMENT_INT;
        long lineOffset = offset + offsetInMask;
        emitAndAdvance(lineOffset);
        return mask & CLEARED[offsetInMask + 1];
    }

    private long loadLong() {
        return segment.get(JAVA_LONG, offset);
    }

    private long loadTail(long count) {
        return LineSegments.bytesAt(segment, offset, Math.toIntExact(count));
    }

    private void emitAndAdvance(long length) {
        action.line(segment, lineStart, length);
        lineStart = length + 1;
    }

    private void transcend(BitwisePartitionHandler next) {
        long nextOffset = next.findFirstLine();
        if (nextOffset < next.limit) { // Next partition contains the next newline
            mergeWithNext(next, nextOffset - 1);
        } else if (next.partition.last()) { // Next partition is the last one and it's missing a newline at the end
            mergeWithNext(next, nextOffset);
        } else { // Next line is in a later partition!
            mergeWithMultiple(next);
        }
    }

    private void mergeWithNext(BitwisePartitionHandler next, long preamble) {
        long trail = limit - lineStart;
        int length = Math.toIntExact(trail + preamble);
        MemorySegment buffer = of(ByteBuffer.allocateDirect(length));
        copy(this.segment, lineStart, buffer, 0, trail);
        copy(next.segment, 0, buffer, trail, preamble);
        emit(buffer, length);
    }

    private void emit(MemorySegment buffer, int length) {
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
        MemorySegment buffer = of(new byte[length]);
        copy(segment, lineStart, buffer, 0, trail);
        long accumulatedSize = trail;
        for (int i = 0; i < mediaries; i++) {
            BitwisePartitionHandler step = collector.get(i);
            copy(
                step.segment,
                0,
                buffer,
                accumulatedSize,
                step.limit
            );
            accumulatedSize += step.limit;
        }
        copy(
            collector.getLast().segment,
            0,
            buffer,
            accumulatedSize,
            lastLineOffset
        );
        emit(buffer, length);
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

    private static long mask(long bytes) {
        long masked = bytes ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
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
                long prebyte = next.segment.get(JAVA_LONG, lo);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + Long.numberOfTrailingZeros(premask) / ALIGNMENT;
                }
                lo += ALIGNMENT;
            }
        }
    }

    @FunctionalInterface
    public interface Mediator extends Function<BitwisePartitioned.Action, BitwisePartitioned.Action> {
    }
}
