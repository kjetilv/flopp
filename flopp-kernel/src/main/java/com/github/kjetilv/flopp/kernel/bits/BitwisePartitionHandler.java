package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ofLength;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class BitwisePartitionHandler implements Runnable, LineSegment {

    private final Partition partition;

    private final Supplier<BitwisePartitionHandler> next;

    private final MemorySegment segment;

    private final BitwisePartitioned.Action action;

    private final long limit;

    private final long logicalLimit;

    private long startIndex;

    private long endIndex;

    private long offset;

    BitwisePartitionHandler(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalSize,
        BitwisePartitioned.Action action,
        Supplier<BitwisePartitionHandler> next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.segment = Objects.requireNonNull(segment, "segment");
        this.offset = offset;
        this.action = Objects.requireNonNull(action, "action");
        this.next = next;

        this.limit = this.partition.length();
        this.logicalLimit = logicalSize;
    }

    @Override
    public void run() {
        try (action) {
            if (processHead()) {
                if (partition.last()) {
                    processTailBody();
                } else {
                    processBody();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed @ " + offset + "/" + startIndex + ": " + action, e);
        }
    }

    @Override
    public String toString() {
        String segmentString = LineSegments.toString(LineSegments.of(segment, startIndex, offset));
        return getClass().getSimpleName() + "[" + partition + " " + segmentString + "]";
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
            long bytes = loadTail();
            long mask = mask(bytes);
            if (mask != 0) {
                return initializeFrom(mask);
            }
            offset += ALIGNMENT;
        }
        return null; // No newline found in the whole partition
    }

    private void processTailBody() {
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        processMain(lastOffset);
        if (tail > 0) {
            processTail();
        }
        if (startIndex < logicalLimit) {
            emitAndAdvance(logicalLimit);
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
        long tail = logicalLimit % ALIGNMENT;
        long lastAligned = logicalLimit - tail;
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
            long mask = mask(loadTail());
            if (mask != 0) { // Tail did not end in newline, send what we got
                shipNextLine(mask);
                return true;
            }
        }
        return false;
    }

    private void processTail() {
        long mask = mask(loadTail());
        if (mask == 0) { // Tail did not end in newline, send what we got
            emitAndAdvance(logicalLimit);
        } else {
            do { // Newlines spotted, ship lines
                mask = shipNextLine(mask);
            } while (mask != 0);
            if (startIndex < logicalLimit) { // Tail did not end in newline, send what we got
                emitAndAdvance(logicalLimit);
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
            long tailBytes = MemorySegments.tail(segment, limit);
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
        if (partition.last() && start + 1 == logicalLimit) {
            return null; // First linebreak was the last
        }
        mask &= CLEARED[(int)(start - offset)];
        if (mask == 0) { // We cleared the current mask
            offset += ALIGNMENT;
        }
        // Mark position of new line
        startIndex = start + 1;
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

    private long loadTail() {
        return MemorySegments.tail(segment, limit);
    }

    @Override
    public long startIndex() {
        return startIndex;
    }

    @Override
    public long endIndex() {
        return endIndex;
    }

    @Override
    public MemorySegment memorySegment() {
        return segment;
    }

    @Override
    public String asString(Charset charset) {
        return asString(null, charset);
    }

    @Override
    public String asString(byte[] buffer, Charset charset) {
        return MemorySegments.fromLongsWithinBounds(segment, startIndex(), endIndex(), buffer, charset);
    }

    @Override
    public long length() {
        return endIndex - startIndex;
    }

    @Override
    public long headStart() {
        return startIndex % ALIGNMENT;
    }

    @Override
    public boolean isAlignedAtStart() {
        return startIndex % ALIGNMENT == 0L;
    }

    @Override
    public boolean isAlignedAtEnd() {
        return endIndex % ALIGNMENT == 0;
    }

    @Override
    public long head(long head) {
        return segment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT) >> head * ALIGNMENT;
    }

    @Override
    public long longNo(long longNo) {
        return segment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT + longNo * ALIGNMENT);
    }

    @Override
    public long bytesAt(long offset, long count) {
        return MemorySegments.bytesAt(memorySegment(), startIndex + offset, count);
    }

    private void emitAndAdvance(long endIndex) {
        this.endIndex = endIndex;
        action.line(this);
        this.startIndex = this.endIndex + 1;
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
        long trailing = limit - startIndex;
        int length = (int)(trailing + nextPrefix);
        MemorySegment buffer = ofLength(length);
        MemorySegment.copy(this.segment, JAVA_BYTE, startIndex, buffer, JAVA_BYTE, 0, trailing);
        MemorySegment.copy(next.segment, JAVA_BYTE, 0, buffer, JAVA_BYTE, trailing, nextPrefix);
        emitMerged(buffer, length, next.partition.last());
    }

    private void mergeWithMultiple(BitwisePartitionHandler next) {
        List<BitwisePartitionHandler> collector = new ArrayList<>();
        long lastLimit = collectAndFindLimit(next, collector);
        combineMultiple(collector, lastLimit);
    }

    private void combineMultiple(List<BitwisePartitionHandler> collector, long lastLineOffset) {
        long trail = limit - startIndex;
        int mediaries = collector.size() - 1;
        long mediarySize = 0L;
        for (int i = 0; i < mediaries; i++) {
            mediarySize += collector.get(i).limit;
        }
        int length = (int)(trail + mediarySize + lastLineOffset);
        MemorySegment buffer = ofLength(length);
        MemorySegment.copy(segment, startIndex, buffer, 0, trail);
        long accumulatedSize = trail;
        for (int i = 0; i < mediaries; i++) {
            BitwisePartitionHandler step = collector.get(i);
            MemorySegment.copy(step.segment, JAVA_BYTE, 0, buffer, JAVA_BYTE, accumulatedSize, step.limit);
            accumulatedSize += step.limit;
        }
        MemorySegment lastSegment = collector.getLast().segment;
        MemorySegment.copy(lastSegment, JAVA_BYTE, 0, buffer, JAVA_BYTE, accumulatedSize, lastLineOffset);
        boolean last = lastLineOffset == lastSegment.byteSize();
        emitMerged(buffer, length, last);
    }

    private void emitMerged(MemorySegment buffer, int length, boolean last) {
        boolean trim = last && buffer.get(JAVA_BYTE, buffer.byteSize() - 1) == '\n';
        action.line(LineSegments.of(buffer, 0, length - (trim ? 1 : 0)));
    }

    private static final int ALIGNMENT = (int) JAVA_LONG.byteSize();

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
            long tail = next.logicalLimit % ALIGNMENT;
            long lastOffset = next.logicalLimit - tail;
            while (lo < lastOffset) {
                long prebyte = next.segment.get(JAVA_LONG, lo);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + Long.numberOfTrailingZeros(premask) / ALIGNMENT;
                }
                lo += ALIGNMENT;
            }
            if (tail > 0) {
                long prebyte = MemorySegments.tail(next.segment, next.limit);
                long premask = mask(prebyte);
                if (premask != 0) {
                    return lo + Long.numberOfTrailingZeros(premask) / ALIGNMENT;
                }
            }
        }
    }

    @FunctionalInterface
    public interface MiddleMan<T> {

        T intercept(T action);
    }
}
