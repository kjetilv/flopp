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
        State state = null;
        try (action) {
            state = processHead();
            if (state == null) {
                return;
            }
            if (partition.last()) {
                processTail(state);
            } else {
                processBody(state);
            }
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed @ \{state}: \{action}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    private State processHead() {
        if (partition.first()) {
            return new State();
        }
        InitState init = initialize();
        if (init == null) {
            return null; // No newlines in this partition
        }
        State state = init.state();
        long mask = init.mask();
        if (mask != 0) {
            do { // Newlines found in current mask
                mask = shipNextLine(state, mask);
            } while (mask != 0);
            advance(state);
        }
        return state;
    }

    private void processBody(State state) {
        processMain(state, limit);
        if (processedOverflow(state)) {
            return; // We found the line in the overflow section
        }
        transcend(state, this.next.get()); // We need to query the next partition
    }

    private InitState initialize() {
        State state = new State();
        while (state.offset < limit) {
            long mask = mask(loadLong(state));
            if (mask != 0) {
                long start = state.offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT + 1;
                mask &= CLEARED[Math.toIntExact(start - state.offset)];
                if (mask == 0) { // We cleared the current mask
                    advance(state);
                }
                mark(state, start); // Mark position of new line
                return new InitState(state, mask); // Return start state
            }
            advance(state);
        }
        return null; // No newline found in the whole partition
    }

    private void processTail(State state) {
        long tail = limit % ALIGNMENT;
        long lastOffset = limit - tail;
        processMain(state, lastOffset);
        if (tail > 0) {
            processTail(state, tail);
        }
    }

    private void processMain(State state, long lastOffset) {
        while (state.offset < lastOffset) {
            long mask = mask(loadLong(state));
            while (mask != 0) {
                mask = shipNextLine(state, mask);
            }
            advance(state);
        }
    }

    private boolean processedOverflow(State state) {
        while (state.offset < physicalLimit) {
            long mask = mask(loadLong(state));
            if (mask == 0) {
                advance(state);
            } else {
                shipNextLine(state, mask);
                return true;
            }
        }
        return false;
    }

    private void processTail(State state, long tail) {
        long mask = mask(loadTail(state, tail));
        if (mask == 0) { // Tail did not end in newline, send what we got
            action.line(segment, state.lineStart, physicalLimit);
        } else {
            do { // Newlines spotted, ship lines
                mask = shipNextLine(state, mask);
            } while (mask != 0);
            if (state.lineStart < physicalLimit) { // Tail did not end in newline, send what we got
                action.line(segment, state.lineStart, physicalLimit);
            }
        }
    }

    private long findFirstLine() {
        long offset = 0L;
        while (offset < limit) {
            long mask = mask(bytesAt(segment, offset));
            if (mask != 0) {
                return offset + Long.numberOfTrailingZeros(mask) / ALIGNMENT + 1;
            }
            offset += ALIGNMENT;
        }
        return limit;
    }

    private long shipNextLine(State state, long mask) {
        int offsetInMask = Long.numberOfTrailingZeros(mask) / ALIGNMENT;
        long lineBreakOffset = state.offset + offsetInMask;
        action.line(segment, state.lineStart, lineBreakOffset);
        mark(state, lineBreakOffset + 1);
        return mask & CLEARED[offsetInMask + 1];
    }

    private long loadLong(State state) {
        return bytesAt(segment, state.offset);
    }

    private long loadTail(State state, long count) {
        long l = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = byteAt(segment, state.offset + i);
            l = (l << ALIGNMENT) + b;
        }
        return l;
    }

    private void transcend(State state, BitwisePartitionHandler next) {
        long nextOffset = next.findFirstLine();
        if (nextOffset < next.limit) { // Next partition contains the next newline
            mergeWithNext(state, next, nextOffset - 1);
        } else if (next.partition.last()) { // Next partition is the last one and it's missing a newline at the end
            mergeWithNext(state, next, nextOffset);
        } else { // Next line is in a later partition!
            mergeWithMultiple(state, next);
        }
    }

    private void mergeWithNext(State state, BitwisePartitionHandler next, long preamble) {
        long trail = limit - state.lineStart;
        int length = Math.toIntExact(trail + preamble);
        MemorySegment buffer = MemorySegment.ofArray(new byte[length]);
        MemorySegment.copy(this.segment, state.lineStart, buffer, 0, trail);
        MemorySegment.copy(next.segment, 0, buffer, trail, preamble);
        action.line(buffer, 0, length);
    }

    private void mergeWithMultiple(State state, BitwisePartitionHandler next) {
        List<BitwisePartitionHandler> collector = new ArrayList<>();
        long lastLimit = collectAndFindLimit(next, collector);
        combineMultiple(state, collector, lastLimit);
    }

    private void combineMultiple(State state, List<BitwisePartitionHandler> collector, long lastLineOffset) {
        long trail = limit - state.lineStart;
        int mediaries = collector.size() - 1;
        long mediarySize = collector.stream()
            .limit(mediaries)
            .mapToLong(spliterator -> spliterator.limit)
            .sum();
        int length = Math.toIntExact(trail + mediarySize + lastLineOffset);
        MemorySegment buffer = MemorySegment.ofArray(new byte[length]);
        MemorySegment.copy(segment, state.lineStart, buffer, 0, trail);
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

    private static void advance(State state) {
        state.offset += ALIGNMENT;
    }

    private static void mark(State state, long pos) {
        state.lineStart = pos;
    }

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
                    return lo + Long.numberOfTrailingZeros(premask) / ALIGNMENT;
                }
                lo += ALIGNMENT;
            }
        }
    }

    private record InitState(State state, long mask) {
    }

    private static class State {

        private long lineStart;

        private long offset;

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[l:\{lineStart}/o:\{offset}]";
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
