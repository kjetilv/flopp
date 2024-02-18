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

    private int columnNo;

    BitwiseLineSplitter(LineSplit lineSplit, Lines lines) {
        Objects.requireNonNull(lineSplit, "lineSplit");

        this.lines = Objects.requireNonNull(lines, "lines");

        this.sepMask = createMask(lineSplit.separator());
        this.quoMask = createMask(lineSplit.quote());
        this.escMask = createMask(lineSplit.escape());

        this.start = new long[lineSplit.columnCount()];
        this.end = new long[lineSplit.columnCount()];
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
        this.segment = segment;
        long length = segment.length();

        startOffset = segment.startIndex();
        try {
            if (length < ALIGNMENT) {
                findSeps(segment.bytesAt(0, length));
                addSep(currentStart, length);
                return;
            }

            long longCount = segment.longCount();
            long headLong = resolveHeadLong(segment);
            findSeps(headLong);

            for (int i = 1; i < longCount; i++) {
                findSeps(segment.getLong(i));
            }

            if (segment.isAlignedAtEnd()) {
                addSep(currentStart, length);
                return;
            }

            findSeps(segment.tail());
            addSep(currentStart, length);
        } finally {
            lines.line(this);

            this.offset = 0;
            this.currentStart = 0;
            this.columnNo = 0;
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
        this.start[columnNo] = startOffset + start;
        this.end[columnNo] = startOffset + end;
        columnNo++;
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

    public interface Lines {

        void line(Line line);
    }
}
