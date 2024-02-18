package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT_INT;

public final class BitwiseLineSplitter implements Consumer<LineSegment>, Origin {

    private final Action action;

    private final long sepMask;

    private final long quoMask;

    private final long escMask;

    private final int[] pickCols;

    private final int endCol;

    private final boolean emitAll;

    private LineSegment segment;

    private final boolean limitedColumns;

    private final boolean pickingColumns;

    private final int pickedColumnCount;

    private long length;

    private long currentStart;

    private long offset;

    private boolean quoting;

    private boolean escaping;

    private int columnNo;

    private int pickedColumnNo;

    private long lineNo;

    BitwiseLineSplitter(char sepChar, char quoChar, char escChar, Action action, int endCol, int[] pickCols) {
        this.action = action;

        this.sepMask = createMask(sepChar);
        this.quoMask = createMask(quoChar);
        this.escMask = createMask(escChar);
        this.pickCols = pickCols == null ? NO_INDICES : pickCols;
        this.endCol = Math.max(0, endCol);

        this.pickedColumnCount = this.pickCols.length;
        this.pickingColumns = this.pickCols.length > 0;
        this.limitedColumns = this.endCol > 0;
        this.emitAll = !(limitedColumns || pickingColumns);
    }

    @Override
    public void accept(LineSegment segment) {
        try {
            this.segment = segment;
            this.length = segment.length();
            this.lineNo = segment.lineNo();

            long longCount = segment.longCount();
            if (segment.isAlignedAtStart() && segment.isEndSafe()) {
                for (int i = 0; i < longCount; i++) {
                    if (eventsDone(segment.longNo(i))) {
                        break;
                    }
                }
                close();
            } else {
                offset = segment.longStart() - segment.startIndex();
                long bytes = segment.getHeadLong();
                for (int i = 1; i < longCount; i++) {
                    if (eventsDone(bytes)) {
                        break;
                    }
                    bytes = segment.longNo(i);
                }
                if (!eventsDone(bytes)) {
                    if (segment.isEndSafe()) {
                        eventsDone(segment.getTailLong());
                    } else {
                        eventsDone(segment.getTail());
                    }
                }
                close();
            }
            action.lineDone(segment.lineNo());
        } finally {
            this.offset = 0;
            this.currentStart = 0;
            this.columnNo = 0;
            this.pickedColumnNo = 0;
        }
    }

    public void close() {
        if (emitAll || !(
            limitedColumns && columnNo == endCol ||
            pickingColumns && pickedColumnCount == pickedColumnNo)
        ) {
            columnAndDone(currentStart, length);
        }
    }

    @Override
    public long ln() {
        return lineNo;
    }

    @Override
    public int col() {
        return columnNo;
    }

    private boolean eventsDone(long bytes) {
        long seps = mask(bytes, sepMask);
        long quos = mask(bytes, quoMask);
        long escs = mask(bytes, escMask);

        int nextSep = distance(seps);
        int nextQuo = distance(quos);
        int nextEsc = distance(escs);

        while (true) {
            int min = Math.min(nextSep, Math.min(nextQuo, nextEsc));
            if (min == ALIGNMENT) {
                offset += ALIGNMENT;
                return offset >= length;
            }
            if (min == nextSep) {
                if (!quoting) {
                    if (escaping) {
                        escaping = false;
                    } else {
                        boolean done = columnAndDone(currentStart, offset + nextSep);
                        currentStart = offset + nextSep + 1;
                        if (done) {
                            return true;
                        }
                    }
                }
                seps &= CLEARED[nextSep];
                nextSep = distance(seps);
            } else if (min == nextQuo) {
                if (escaping) {
                    escaping = false;
                } else {
                    quoting = !quoting;
                }
                quos &= CLEARED[nextQuo];
                nextQuo = distance(quos);
            } else {
                escaping = true;
                escs &= CLEARED[nextEsc];
                nextEsc = distance(escs);
            }
        }
    }

    private boolean columnAndDone(long prev, long next) {
        if (emitAll) {
            ship(columnNo, next, prev);
            return false;
        }
        if (endCol > 0) {
            if (columnNo < endCol) {
                ship(columnNo, next, prev);
            }
            columnNo++;
            return columnNo == endCol;
        }
        if (pickCols[pickedColumnNo] == columnNo) {
            ship(columnNo, next, prev);
            pickedColumnNo++;
        }
        columnNo++;
        return pickedColumnNo == pickCols.length;
    }

    private void ship(int column, long pos, long previous) {
        this.columnNo = column;
        action.cell(
            this,
            segment.memorySegment(),
            segment.startIndex() + previous,
            segment.startIndex() + pos
        );
    }

    public static final char DEFAULT_QUOTE = '"';

    public static final char DEFAULT_ESC = '\\';

    public static final int[] NO_INDICES = new int[0];

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

    private static long createMask(char s) {
        long mask = s;
        for (int i = 0; i < ALIGNMENT; i++) {
            mask = (mask << ALIGNMENT) + s;
        }
        return mask;
    }

    private static long mask(long bytes, long mask) {
        long masked = bytes ^ mask;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static int distance(long bytes) {
        return Long.numberOfTrailingZeros(bytes) / ALIGNMENT_INT;
    }

    public interface Action {

        void cell(Origin origin, MemorySegment segment, long startIndex, long endIndex);

        default void lineDone(long line) {
        }
    }
}
