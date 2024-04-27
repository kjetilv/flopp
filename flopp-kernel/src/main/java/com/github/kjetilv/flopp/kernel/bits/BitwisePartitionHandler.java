package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static com.github.kjetilv.flopp.kernel.MemorySegments.of;
import static java.lang.foreign.MemorySegment.copy;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
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
    }

    @Override
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
            throw new IllegalStateException(this + " failed @ " + offset + "/" + lineStart + ": " + action, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + " " + asString() + "]";
    }

    public String asString() {
        return LineSegments.asString(segment, lineStart, offset);
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
        long steps = (lastOffset - offset) / ALIGNMENT;
        for (long l = 0; l < steps; l++) {
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
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        while (offset < lastOffset) {
            long bytes = segment.get(JAVA_LONG, offset);
            long mask = mask(bytes);
            if (mask != 0) {
                return offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT;
            }
            offset += ALIGNMENT;
        }
        if (tail > 0) {
            long tailBytes = LineSegments.bytesAt(segment, offset, tail);
            long mask = mask(tailBytes);
            if (mask != 0) {
                return offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT;
            }
        }
        return limit;
    }

    private Long initializeFrom(long bytes) {
        long mask = bytes;
        long start = offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT;
        if (partition.last() && start + 1 == physicalLimit) {
            return null; // First linebreak was the last
        }
        mask &= CLEARED[Math.toIntExact(start - offset)];
        if (mask == 0) { // We cleared the current mask
            offset += ALIGNMENT;
        }
        // Mark position of new line
        lineStart = start + 1;
        return mask;
    }

    private long shipNextLine(long mask) {
        int offsetInMask = Long.numberOfTrailingZeros(mask) / ALIGNMENT;
        long lineOffset = offset + offsetInMask;
        emitAndAdvance(lineOffset);
        return mask & CLEARED[offsetInMask];
    }

    private long loadLong() {
        return segment.get(JAVA_LONG, offset);
    }

    private long loadTail(long count) {
        return LineSegments.bytesAt(segment, offset, Math.toIntExact(count));
    }

    private void emitAndAdvance(long endIndex) {
        action.line(segment, lineStart, endIndex);
        lineStart = endIndex + 1;
    }

    private void transcend(BitwisePartitionHandler next) {
        long nextOffset = next.findFirstLine();
        if (nextOffset < next.limit) { // Next partition contains the next newline
            mergeWithNext(next, nextOffset);
        } else if (next.partition.last()) { // Next partition is the last one and it's missing a newline at the end
            mergeWithNext(next, nextOffset);
        } else { // Next line is in a later partition!
            mergeWithMultiple(next);
        }
    }

    private void mergeWithNext(BitwisePartitionHandler next, long nextPrefix) {
        long trailing = limit - lineStart;
        int length = Math.toIntExact(trailing + nextPrefix);
        MemorySegment buffer = of(ByteBuffer.allocateDirect(length));
        copy(this.segment, lineStart, buffer, 0, trailing);
        copy(next.segment, 0, buffer, trailing, nextPrefix);
        emitMerged(buffer, length, next.partition.last());
    }

    private void mergeWithMultiple(BitwisePartitionHandler next) {
        List<BitwisePartitionHandler> collector = new ArrayList<>();
        long lastLimit = collectAndFindLimit(next, collector);
        combineMultiple(collector, lastLimit);
    }

    private void combineMultiple(List<BitwisePartitionHandler> collector, long lastLineOffset) {
        long trail = limit - lineStart;
        int mediaries = collector.size() - 1;
        long mediarySize = 0L;
        for (int i = 0; i < mediaries; i++) {
            mediarySize += collector.get(i).limit;
        }
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
        MemorySegment lastSegment = collector.getLast().segment;
        copy(
            lastSegment,
            0,
            buffer,
            accumulatedSize,
            lastLineOffset
        );
        emitMerged(buffer, length, lastLineOffset == lastSegment.byteSize());
    }

    private void emitMerged(MemorySegment buffer, int length, boolean last) {
        boolean trim = last && buffer.get(JAVA_BYTE, buffer.byteSize() - 1) == '\n';
        action.line(buffer, 0, length - (trim ? 1 : 0));
    }

    private static final int ALIGNMENT = Math.toIntExact(JAVA_LONG.byteSize());

    private static final long[] CLEARED = {
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
            if (next.next == null) {
                return next.limit;
            }
            BitwisePartitionHandler nextNext = next.next.get();
            if (nextNext == null) {
                return next.limit;
            }
            next = nextNext;
            collector.add(next);
            long lo = 0L;
            long tail = next.physicalLimit % ALIGNMENT;
            long lastOffset = next.physicalLimit - tail;
            while (lo < lastOffset) {
                long prebyte = next.segment.get(JAVA_LONG, lo);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + Long.numberOfTrailingZeros(premask) / ALIGNMENT;
                }
                lo += ALIGNMENT;
            }
            if (tail > 0) {
                long prebyte = LineSegments.bytesAt(next.segment, lo, tail);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + Long.numberOfTrailingZeros(premask) / ALIGNMENT;
                }
            }
        }
    }

    @FunctionalInterface
    public interface Mediator {

        BitwisePartitioned.Action mediate(BitwisePartitioned.Action action);
    }
}
