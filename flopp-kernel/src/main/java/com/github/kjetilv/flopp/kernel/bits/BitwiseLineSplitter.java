package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

public final class BitwiseLineSplitter implements Consumer<LineSegment> {

    private final BitwisePartitioned.Action action;

    private final int[] indexes;

    private final long splitMask;

    private final long quoteMask;

    public BitwiseLineSplitter(
        char splitCharacter,
        BitwisePartitioned.Action action,
        int... indexes
    ) {
        this(splitCharacter, (char) 0, action, indexes);
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        BitwisePartitioned.Action action,
        int... indexes
    ) {
        this.action = action;
        this.indexes = indexes;

        this.quoteMask = quoteChar == 0x00 ? 0L : createMask(quoteChar);
        this.splitMask = createMask(splitCharacter);
    }

    @Override
    public void accept(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        int alignedSteps = length / ALIGNMENT;
        int tail = length % ALIGNMENT;

        State state = new State(segment, action, indexes, splitMask, quoteMask);

        for (int i = 0; i < alignedSteps; i++) {
            long bytes = state.loadLong();
            if (state.completed(bytes)) {
                return;
            }
            state.ffwd();
        }
        long bytes = state.loadBytes(tail);
        if (state.completed(bytes)) {
            return;
        }
        if (state.trails(length)) {
            state.flush(length);
        }
    }

    private static final int ALIGNMENT = 0x08;

    private static long createMask(char s) {
        long mask = s;
        for (int i = 0; i < ALIGNMENT; i++) {
            mask = (mask << ALIGNMENT) + s;
        }
        return mask;
    }

    private static final class State {

        private final LineSegment lineSegment;

        private final BitwisePartitioned.Action action;

        private final int[] indexedColumns;

        private final long splitMask;

        private final long quoteMask;

        private final boolean emitAll;

        private final MemorySegment memorySegment;

        private int lastStart;

        private int longOffset;

        private int currentColumn;

        private int indexedColumnNo;

        private int nextIndexedColumnNo;

        private State(
            LineSegment lineSegment,
            BitwisePartitioned.Action action,
            int[] indexedColumns,
            long splitMask,
            long quoteMask
        ) {
            this.lineSegment = lineSegment;
            this.indexedColumns = indexedColumns;
            this.emitAll = this.indexedColumns.length == 0;
            this.action = action;
            this.memorySegment = lineSegment.memorySegment();
            this.splitMask = splitMask;
            this.quoteMask = quoteMask;

            this.nextIndexedColumnNo = this.emitAll ? -1 : indexedColumns[indexedColumnNo];
        }

        private boolean trails(int length) {
            return (emitAll || currentColumn == nextIndexedColumnNo) && lastStart < length;
        }

        private void ffwd() {
            longOffset += ALIGNMENT;
        }

        private long loadLong() {
            return lineSegment.longAt(longOffset);
        }

        private long loadBytes(int tail) {
            long l = 0;
            for (long i = (long) tail - 1; i >= 0; i--) {
                byte b = lineSegment.byteAt(longOffset + i);
                l = (l << Bits.ALIGNMENT) + b;
            }
            return l;
        }

        private void flush(int length) {
            emitLine(length);
        }

        private boolean completed(long bytes) {
            long positions = mask(bytes, splitMask);
            while (positions != 0) {
                if (foundLast(positions)) {
                    return true;
                }
                positions &= clear(lastStart, longOffset);
            }
            return false;
        }

        private boolean foundLast(long mask) {
            int increment = increment(mask);
            int nextEnd = longOffset + increment;
            if (emitAll) {
                emitLine(nextEnd);
            } else if (currentColumn == nextIndexedColumnNo) {
                emitLine(nextEnd);
                indexedColumnNo++;
                if (indexedColumnNo == indexedColumns.length) {
                    return true;
                }
                nextIndexedColumnNo = indexedColumns[indexedColumnNo];
            }
            lastStart = nextEnd + 1;
            currentColumn++;
            return false;
        }

        private void emitLine(int endPosition) {
            action.line(
                memorySegment,
                lineSegment.index(lastStart),
                lineSegment.index(endPosition)
            );
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

        private static long mask(long bytes, long mask) {
            long masked = bytes ^ mask;
            long underflown = masked - 0x0101010101010101L;
            long clearedHighBits = underflown & ~masked;
            return clearedHighBits & 0x8080808080808080L;
        }

        private static int increment(long mask) {
            int increment = Long.numberOfTrailingZeros(mask) / ALIGNMENT;
            return increment;
        }

        private static long clear(long lastStart, long offset) {
            return CLEARED[Math.toIntExact(lastStart - offset)];
        }
    }
}
