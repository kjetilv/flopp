package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT_INT;

public final class BitwiseLineSplitter implements Consumer<LineSegment>, CommaSeparatedLine {

    private final Consumer<CommaSeparatedLine> lines;

    private final long sepMask;

    private final long quoMask;

    private final long escMask;

    private final long[] start;

    private final long[] end;

    private LineSegment segment;

    private long currentStart;

    private long startOffset;

    private long offset;

    private boolean quoting;

    private boolean escaping;

    private boolean quoted;

    private int columnNo;

    BitwiseLineSplitter(LinesFormat linesFormat, Consumer<CommaSeparatedLine> lines) {
        Objects.requireNonNull(linesFormat, "lineSplit");

        this.lines = Objects.requireNonNull(lines, "lines");

        this.sepMask = createMask(linesFormat.separator());
        this.quoMask = createMask(linesFormat.quote());
        this.escMask = createMask(linesFormat.escape());

        this.start = new long[linesFormat.columnCount()];
        this.end = new long[linesFormat.columnCount()];
    }

    @Override
    public MemorySegment memorySegment() {
        return segment.memorySegment();
    }

    @Override
    public int columnCount() {
        return columnNo;
    }

    @Override
    public long[] start() {
        return start;
    }

    @Override
    public long[] end() {
        return end;
    }

    @Override
    public void accept(LineSegment segment) {
        this.offset = this.currentStart = this.columnNo = 0;
        this.quoted = this.quoting = this.escaping = false;

        this.segment = Objects.requireNonNull(segment, "segment");
        this.startOffset = this.segment.startIndex();

        long length = this.segment.length();
        if (length < ALIGNMENT) {
            findSeps(this.segment.bytesAt(0, length));
            addSep(length);
        } else {
            long longCount = this.segment.longCount();
            long headLong = resolveHeadLong(this.segment);
            findSeps(headLong);
            for (int i = 1; i < longCount; i++) {
                findSeps(this.segment.longNo(i));
            }
            if (this.segment.isAlignedAtEnd()) {
                addSep(length);
            } else {
                findSeps(this.segment.tail());
                addSep(length);
            }
        }
        lines.accept(this);
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{segment.asString()}]";
    }

    private long resolveHeadLong(LineSegment segment) {
        long headStart = segment.headStart();
        if (headStart == 0) {
            return segment.longNo(0);
        }
        offset = -headStart;
        return segment.head(headStart);
    }

    private void findSeps(long bytes) {
        long seps = mask(bytes, sepMask);
        long quos = mask(bytes, quoMask);
        long escs = mask(bytes, escMask);

        int nextSep = dist(seps);
        int nextQuo = dist(quos);
        int nextEsc = dist(escs);

        while (true) {
            if (nextEsc == ALIGNMENT_INT) {
                if ((nextSep & nextQuo) == ALIGNMENT_INT) {
                    offset += ALIGNMENT_INT;
                    return;
                }
                int min = Math.min(nextSep, nextQuo);
                if (min == nextSep) {
                    handleSep(nextSep);
                    seps &= CLEARED[nextSep];
                    nextSep = dist(seps);
                } else {
                    handleQuo();
                    quos &= CLEARED[nextQuo];
                    nextQuo = dist(quos);
                }
            } else {
                int min = Math.min(nextSep, Math.min(nextQuo, nextEsc));
                if (min == nextSep) {
                    handleSep(nextSep);
                    seps &= CLEARED[nextSep];
                    nextSep = dist(seps);
                } else if (min == nextQuo) {
                    handleQuo();
                    quos &= CLEARED[nextQuo];
                    nextQuo = dist(quos);
                } else {
                    escaping = true;
                    escs &= CLEARED[nextEsc];
                    nextEsc = dist(escs);
                }
            }
        }
    }

    private void handleSep(int nextSep) {
        if (quoting) {
            return;
        }
        if (escaping) {
            escaping = false;
        } else {
            addSep(offset + nextSep);
            currentStart = offset + nextSep + 1;
        }
    }

    private void handleQuo() {
        if (escaping) {
            escaping = false;
        } else {
            quoting = !quoting;
            quoted = true;
        }
    }

    private void addSep(long end) {
        int quoteOffset = quoted ? 1 : 0;
        this.start[columnNo] = startOffset + currentStart + quoteOffset;
        this.end[columnNo] = startOffset + end - quoteOffset;
        columnNo++;
        quoted = false;
    }

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

    private static int dist(long bytes) {
        return Long.numberOfTrailingZeros(bytes) / ALIGNMENT_INT;
    }
}
