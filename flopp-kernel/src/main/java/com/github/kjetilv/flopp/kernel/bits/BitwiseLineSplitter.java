package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.bits.Bits.ALIGNMENT_INT;

public final class BitwiseLineSplitter implements Consumer<LineSegment>, Line {

    private final Lines lines;

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

    BitwiseLineSplitter(LinesFormat linesFormat, Lines lines) {
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
    public int columns() {
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
        try {
            this.offset = this.currentStart = this.columnNo = 0;
            this.quoted = this.quoting = this.escaping = false;

            this.segment = Objects.requireNonNull(segment, "segment");
            this.startOffset = this.segment.startIndex();

            long length = this.segment.length();
            if (length < ALIGNMENT) {
                findSeps(this.segment.bytesAt(0, length));
                addSep(currentStart, length);
            } else {
                long longCount = this.segment.longCount();
                long headLong = resolveHeadLong(this.segment);
                findSeps(headLong);

                for (int i = 1; i < longCount; i++) {
                    findSeps(this.segment.getLong(i));
                }

                if (this.segment.isAlignedAtEnd()) {
                    addSep(currentStart, length);
                } else {
                    findSeps(this.segment.tail());
                    addSep(currentStart, length);
                }
            }
        } finally {
            lines.line(this);
        }
    }

    private long resolveHeadLong(LineSegment segment) {
        long headStart = segment.headStart();
        if (headStart == 0) {
            return segment.getLong(0);
        }
        offset = -headStart;
        return segment.head(headStart);
    }

    private void findSeps(long bytes) {
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
                return;
            }
            if (min == nextSep) {
                if (!quoting) {
                    if (escaping) {
                        escaping = false;
                    } else {
                        addSep(currentStart, offset + nextSep);
                        currentStart = offset + nextSep + 1;
                    }
                }
                seps &= CLEARED[nextSep];
                nextSep = distance(seps);
            } else if (min == nextQuo) {
                if (escaping) {
                    escaping = false;
                } else {
                    quoting = !quoting;
                    quoted = true;
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

    private void addSep(long start, long end) {
        int quoteOffset = quoted ? 1 : 0;
        this.start[columnNo] = startOffset + start + quoteOffset;
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

    private static int distance(long bytes) {
        return Long.numberOfTrailingZeros(bytes) / ALIGNMENT_INT;
    }
}
