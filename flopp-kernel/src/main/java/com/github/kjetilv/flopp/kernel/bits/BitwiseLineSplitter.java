package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

@SuppressWarnings("unused")
public final class BitwiseLineSplitter implements Consumer<LineSegment>, Origin {

    private final Action action;

    private final long sepMask;

    private final long quoMask;

    private final long escMask;

    private final int[] indexes;

    private final int endColumn;

    private final boolean emitAll;

    private LineSegment segment;

    private int length;

    private int lastSep;

    private int lastOffset;

    private int tail;

    private int offset;

    private boolean quoting;

    private boolean escaping;

    private int columnCount;

    private int indexedColumnCount;

    private long lineNo;

    private int columnNo;

    private boolean done;

    BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        char escapeChar,
        Action action,
        int endColumn,
        int[] indexes
    ) {
        this.action = action;

        this.sepMask = createMask(splitCharacter);
        this.quoMask = createMask(quoteChar);
        this.escMask = createMask(escapeChar);
        this.indexes = indexes == null ? NO_INDICES : indexes;
        this.endColumn = endColumn;

        this.emitAll = this.endColumn <= 0 && this.indexes.length == 0;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void accept(LineSegment segment) {
        this.segment = segment;
        this.length = Math.toIntExact(segment.length());
        this.tail = length % ALIGNMENT;
        this.lastOffset = length - tail;
        while (!eventsDone()) {
        }
        if (!done) {
            close();
        }
        action.lineDone(segment.lineNo());
    }

    public boolean esc(int pos) {
        escaping = true;
        return false;
    }

    public void quo(int pos) {
        if (escaping) {
            escaping = false;
        } else {
            quoting = !quoting;
        }
    }

    public void close() {
        columnAndDone(length);
    }

    @Override
    public int file() {
        return 1;
    }

    @Override
    public long line() {
        return lineNo;
    }

    @Override
    public int column() {
        return columnNo;
    }

    private boolean sep(int pos) {
        if (!quoting) {
            if (escaping) {
                escaping = false;
            } else {
                boolean done = columnAndDone(pos);
                lastSep = pos + 1;
                return done;
            }
        }
        return false;
    }

    private boolean eventsDone() {
        if (done) {
            return true;
        }
        long bytes = loadLong();

        long seps = mask(bytes, sepMask);
        long quos = mask(bytes, quoMask);
        long escs = mask(bytes, escMask);

        int nextSep = distance(seps);
        int nextQuo = distance(quos);
        int nextEsc = distance(escs);

        while (true) {
            switch (min(nextSep, nextQuo, nextEsc)) {
                case SEP -> {
                    if (sep(offset + nextSep)) {
                        return done = true;
                    }
                    seps &= CLEARED[nextSep];
                    nextSep = distance(seps);
                }
                case QUO -> {
                    quo(offset + nextQuo);
                    quos &= CLEARED[nextQuo];
                    nextQuo = distance(quos);
                }
                case ESC -> {
                    escaping = true;
                    escs &= CLEARED[nextEsc];
                    nextEsc = distance(escs);
                }
                case _I_ -> {
                    offset += ALIGNMENT;
                    done = offset >= length;
                    if (done) {
                        close();
                    }
                    return done;
                }
            }
        }
    }

    private long loadLong() {
        if (offset < lastOffset) {
            return segment.longAt(offset);
        }
        long l = 0;
        for (int i = tail - 1; i >= 0; i--) {
            byte b = segment.byteAt(offset + i);
            l = (l << Bits.ALIGNMENT) + b;
        }
        return l;
    }

    private boolean columnAndDone(int pos) {
        if (emitAll) {
            ship(columnCount, pos);
            return false;
        }
        if (endColumn > 0) {
            if (columnCount < endColumn) {
                ship(columnCount, pos);
            }
            columnCount++;
            return columnCount == endColumn;
        }
        if (indexes[indexedColumnCount] == columnCount) {
            ship(columnCount, pos);
            indexedColumnCount++;
        }
        columnCount++;
        return indexedColumnCount == indexes.length;
    }

    private void ship(int column, int pos) {
        this.lineNo = segment.lineNo();
        this.columnNo = column;
        action.cell(
            this,
            segment.memorySegment(),
            segment.startIndex() + lastSep,
            segment.startIndex() + pos
        );
    }

    public static final char DEFAULT_QUOTE = '"';

    public static final char DEFAULT_ESC = '\\';

    public static final int[] NO_INDICES = new int[0];

    private static final int ALIGNMENT = 0x08;

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

    private static Token min(int sep, int quo, int esc) {
        int min = Math.min(sep, Math.min(quo, esc));
        if (min < ALIGNMENT) {
            if (min == sep) {
                return Token.SEP;
            }
            if (min == quo) {
                return Token.QUO;
            }
            return Token.ESC;
        }
        return Token._I_;
    }

    private static long mask(long bytes, long mask) {
        long masked = bytes ^ mask;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static int distance(long bytes) {
        return Long.numberOfTrailingZeros(bytes) / ALIGNMENT;
    }

    public interface Action {

        void cell(Origin origin, MemorySegment segment, long startIndex, long endIndex);

        default void lineDone(long line) {
        }

        default void fileDone(long file) {
        }
    }

    public enum Token {

        SEP,

        QUO,

        ESC,

        _I_
    }
}
