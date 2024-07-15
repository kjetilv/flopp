package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvQuotedSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder quoFinder;

    private int state;

    BitwiseCsvQuotedSplitter(Consumer<SeparatedLine> lines, CsvFormat.Quoted format) {
        super(lines, format);
        this.quoFinder = Bits.finder(format.quote(), format.fast());
    }

    @Override
    protected void separate(LineSegment segment) {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
        this.state = STARTING_COLUMN;
        this.startOffset = segment.startIndex();

        long length = segment.length();
        if (length < MemorySegments.ALIGNMENT) {
            findSeps(segment.bytesAt(0, length), 0);
            markSeparator(length);
        } else {
            long headStart = this.startOffset % ALIGNMENT_INT;
            processHead(segment, headStart);
            long start = this.startOffset - headStart + ALIGNMENT_INT;
            long end = segment.alignedEnd();
            for (long i = start; i < end; i += ALIGNMENT_INT) {
                findSeps(segment.longAt(i), 0);
            }
            if (segment.isAlignedAtEnd()) {
                markSeparator(length);
            } else {
                findSeps(segment.tail(), 0);
                markSeparator(length);
            }
        }
    }

    private void processHead(LineSegment segment, long headStart) {
        if (headStart == 0) {
            long headLong = segment.longNo(0);
            findSeps(headLong, 0);
        } else {
            offset = -headStart;
            long headLong = segment.head(headStart);
            findSeps(headLong, headStart);
        }
    }

    private void findSeps(long bytes, long shift) {
        int sep = sepFinder.next(bytes);
        int quo = quoFinder.next(bytes);

        while (true) {
            int diff = sep - quo;
            if (diff == 0) {
                offset += MemorySegments.ALIGNMENT;
                return;
            }
            if (diff < 0) {
                handleSep(sep + shift);
                sep = sepFinder.next();
            } else {
                handleQuo();
                quo = quoFinder.next();
            }
        }
    }

    private void handleSep(long nextSep) {
        long index = offset + nextSep;
        switch (state) {
            case STARTING_COLUMN -> {
                markSeparator(index);
                currentStart = index;
            }
            case QUOTING_QUOTE -> {
                markSeparator(index);
                currentStart = index;
                state = STARTING_COLUMN;
            }
        }
    }

    private void handleQuo() {
        state = switch (state) {
            case STARTING_COLUMN, QUOTING_QUOTE -> QUOTING_COLUMN;
            case QUOTING_COLUMN -> QUOTING_QUOTE;
            default ->
                throw new IllegalStateException("Wrong state: " + state);
        };
    }

    private static final int STARTING_COLUMN = 1;

    private static final int QUOTING_QUOTE = 2;

    private static final int QUOTING_COLUMN = 4;
}
