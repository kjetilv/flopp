package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_INT;
import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_POW;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class BitwisePartitionLineFeeder implements Runnable, LineSegment {

    private final Partition partition;

    private final Supplier<BitwisePartitionLineFeeder> next;

    private final MemorySegment segment;

    private final Bits.Finder finder;

    private final Consumer<LineSegment> action;

    private final long limit;

    private final long logicalLimit;

    private volatile long firstLine = -1;

    private long startIndex;

    private long endIndex;

    private long offset;

    private final boolean first;

    private final boolean last;

    BitwisePartitionLineFeeder(
        Partition partition,
        MemorySegment segment,
        long offset,
        long logicalSize,
        Consumer<LineSegment> action,
        Supplier<BitwisePartitionLineFeeder> next
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.first = this.partition.first();
        this.last = this.partition.last();
        this.segment = Objects.requireNonNull(segment, "segment");
        this.finder = Bits.swarFinder('\n');
        this.offset = offset;
        this.action = Objects.requireNonNull(action, "action");
        this.next = next;

        this.limit = this.partition.length();
        this.logicalLimit = logicalSize;
    }

    @Override
    public void run() {
        try {
            if (first || processedHead()) {
                if (last) {
                    processTailBody();
                } else {
                    processMainBody(limit);
                    if (!processedOverflow()) {
                        processNextPartition(this.next.get());
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed @ " + offset + "/" + startIndex + ": " + action, e);
        }
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

    private boolean processedHead() {
        while (offset < limit) {
            try {
                long data = segment.get(JAVA_LONG, offset);
                int dist = finder.next(data);
                if (dist == ALIGNMENT_INT) {
                    continue;
                }
                // Found newline
                return processInitialLineAt(offset + dist);
            } finally {
                offset += ALIGNMENT_INT;
            }
        }
        return false; // No newline found in the whole partition
    }

    private void processTailBody() {
        long tail = limit % ALIGNMENT_INT;
        long lastOffset = limit - tail;
        processMainBody(lastOffset);
        if (tail > 0) {
            processTail();
        }
        if (startIndex < logicalLimit) {
            cycle(logicalLimit);
        }
    }

    private void processMainBody(long lastOffset) {
        long steps = lastOffset - offset >> ALIGNMENT_POW;
        for (int i = 0; i < steps; i++) {
            long data = segment.get(JAVA_LONG, offset);
            int dist = finder.next(data);
            while (dist != ALIGNMENT_INT) {
                cycle(offset + dist);
                dist = finder.next();
            }
            offset += ALIGNMENT_INT;
        }
    }

    private boolean processInitialLineAt(long start) {
        this.startIndex = start + 1; // Mark position of new line
        this.firstLine = this.startIndex;
        if (last && start + 1 == logicalLimit) { // First linebreak was also EOF
            return false;
        }
        int next;
        while ((next = finder.next()) != ALIGNMENT_INT) {
            cycle(offset + next);
        }
        return true;
    }

    private boolean processedOverflow() {
        long tail = logicalLimit % ALIGNMENT_INT;
        long lastAligned = logicalLimit - tail;
        while (offset < lastAligned) {
            long data = segment.get(JAVA_LONG, offset);
            int dist = finder.next(data);
            if (dist != ALIGNMENT_INT) {
                long lineOffset = offset + dist;
                cycle(lineOffset);
                return true;
            }
            offset += ALIGNMENT_INT;
        }
        if (tail > 0) {
            long data = loadTail();
            int dist = finder.next(data);
            if (dist < ALIGNMENT_INT) { // Tail did not end in newline, send what we got
                cycle(offset + dist);
                return true;
            }
        }
        return false;
    }

    private void processTail() {
        long data = loadTail();
        int dist = finder.next(data);
        if (dist < ALIGNMENT_INT) {
            do { // Newlines spotted, ship lines
                cycle(offset + dist);
                dist = finder.next();
            } while (dist < ALIGNMENT_INT);
            if (startIndex < logicalLimit) { // Tail did not end in newline, send what we got
                cycle(logicalLimit);
            }
        } else { // Tail did not end in newline, send what we got
            cycle(logicalLimit);
        }
    }

    private void cycle(long index) {
        this.endIndex = index;
        this.action.accept(this);
        this.startIndex = index + 1;
    }

    private long loadTail() {
        return MemorySegments.tail(segment, limit);
    }

    private void processNextPartition(BitwisePartitionLineFeeder next) {
        long nextOffset = next.firstLine >= 0 ? next.firstLine : next.findFirstLine(finder);
        if (next.containsLine(nextOffset)) { // Next partition contains the next newline
            mergeWithNext(next, nextOffset);
        } else { // Next line is in a later partition!
            mergeWithMultiple(next);
        }
    }

    private long findFirstLine(Bits.Finder finder) {
        long offset = 0L;
        long tail = limit % ALIGNMENT_INT;
        long lastOffset = limit - tail;
        while (offset < lastOffset) {
            long data = segment.get(JAVA_LONG, offset);
            int dist = finder.next(data);
            if (dist != ALIGNMENT_INT) {
                return offset + dist;
            }
            offset += ALIGNMENT_INT;
        }
        if (tail > 0) {
            long data = MemorySegments.tail(segment, limit);
            int dist = finder.next(data);
            if (dist != ALIGNMENT_INT) {
                return offset + dist;
            }
        }
        return limit;
    }

    private boolean containsLine(long nextOffset) {
        return nextOffset < limit || last;
    }

    private void mergeWithNext(BitwisePartitionLineFeeder next, long nextOffset) {
        long trailing = limit - startIndex;
        int length = (int) (trailing + nextOffset);
        MemorySegment buffer = MemorySegments.ofLength(length);
        MemorySegments.copyBytes(this.segment, startIndex, buffer, 0, trailing);
        MemorySegments.copyBytes(next.segment, buffer, trailing, nextOffset);
        emitMerged(buffer, length, next.last);
    }

    private void mergeWithMultiple(BitwisePartitionLineFeeder next) {
        List<BitwisePartitionLineFeeder> collector = new ArrayList<>();
        long lastLimit = collectAndFindLimit(finder, next, collector);
        combineMultiple(collector, lastLimit);
    }

    private void combineMultiple(List<BitwisePartitionLineFeeder> collector, long lastLineOffset) {
        long trail = limit - startIndex;
        int mediaries = collector.size() - 1;
        long mediarySize = 0L;
        for (int i = 0; i < mediaries; i++) {
            mediarySize += collector.get(i).limit;
        }
        int length = (int) (trail + mediarySize + lastLineOffset);
        MemorySegment buffer = MemorySegments.ofLength(length);
        MemorySegments.copyBytes(segment, startIndex, buffer, 0, trail);
        long accumulatedSize = trail;
        for (int i = 0; i < mediaries; i++) {
            BitwisePartitionLineFeeder step = collector.get(i);
            MemorySegments.copyBytes(step.segment, buffer, accumulatedSize, step.limit);
            accumulatedSize += step.limit;
        }
        MemorySegment lastSegment = collector.getLast().segment;
        MemorySegments.copyBytes(lastSegment, buffer, accumulatedSize, lastLineOffset);
        boolean last = lastLineOffset == lastSegment.byteSize();
        emitMerged(buffer, length, last);
    }

    private void emitMerged(MemorySegment buffer, int length, boolean last) {
        boolean trim = last && buffer.get(JAVA_BYTE, buffer.byteSize() - 1) == '\n';
        action.accept(LineSegments.of(buffer, 0, length - (trim ? 1 : 0)));
    }

    private static long collectAndFindLimit(
        Bits.Finder finder,
        BitwisePartitionLineFeeder resolvedNext,
        List<BitwisePartitionLineFeeder> collector
    ) {
        BitwisePartitionLineFeeder next = resolvedNext;
        collector.add(next);
        while (true) {
            if (next.next == null) {
                return next.limit;
            }
            BitwisePartitionLineFeeder nextNext = next.next.get();
            if (nextNext == null) {
                return next.limit;
            }
            next = nextNext;
            collector.add(next);
            long lo = 0L;
            long tail = next.logicalLimit % ALIGNMENT_INT;
            long lastOffset = next.logicalLimit - tail;
            while (lo < lastOffset) {
                long data = next.segment.get(JAVA_LONG, lo);
                int dist = finder.next(data);
                if (dist < ALIGNMENT_INT) {
                    return lo + dist;
                }
                lo += ALIGNMENT_INT;
            }
            if (tail > 0) {
                long data = MemorySegments.tail(next.segment, next.limit);
                int dist = finder.next(data);
                if (dist < ALIGNMENT_INT) {
                    return lo + dist;
                }
            }
        }
    }

    @Override
    public String toString() {
        String segmentString = LineSegments.toString(this);
        return getClass().getSimpleName() + "[" + partition + " " + segmentString + "]";
    }
}
