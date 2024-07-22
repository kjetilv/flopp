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
    void separate(LineSegment segment) {
        this.state = STARTING_COLUMN;

        long length = segment.length();
        if (length < MemorySegments.ALIGNMENT) {
            findSeps(segment.bytesAt(0, length));
        } else {
            long shift = this.startOffset % ALIGNMENT_INT;
            findInitialSeps(segment.head(shift), shift);
            long start = this.startOffset - shift + ALIGNMENT_INT;
            long end = segment.alignedEnd();
            for (long i = start; i < end; i += ALIGNMENT_INT) {
                findSeps(segment.longAt(i));
            }
            findSeps(segment.tail());
        }
    }

    private void findInitialSeps(long data, long shift) {
        offset = -shift;

        int sep = sepFinder.next(data);
        int quo = quoFinder.next(data);
        while (true) {
            int diff = sep - quo;
            if (diff == 0) {
                offset += MemorySegments.ALIGNMENT;
                return;
            }
            if (diff < 0) {
                handleSep(offset + sep + shift);
                sep = sepFinder.next();
            } else {
                handleQuo();
                quo = quoFinder.next();
            }
        }
    }

    private void findSeps(long data) {
        int sep = sepFinder.next(data);
        int quo = quoFinder.next(data);
        while (true) {
            int diff = sep - quo;
            if (diff == 0) {
                offset += MemorySegments.ALIGNMENT;
                return;
            }
            if (diff < 0) {
                handleSep(offset + sep);
                sep = sepFinder.next();
            } else {
                handleQuo();
                quo = quoFinder.next();
            }
        }
    }

    private void handleSep(long index) {
        switch (state) {
            case STARTING_COLUMN -> {
                markSeparator(index);
            }
            case QUOTING_QUOTE -> {
                markSeparator(index);
                state = STARTING_COLUMN;
            }
        }
    }

    private void handleQuo() {
        state = switch (state) {
            case STARTING_COLUMN, QUOTING_QUOTE -> QUOTING_COLUMN;
            case QUOTING_COLUMN -> QUOTING_QUOTE;
            default -> throw new IllegalStateException("Wrong state: " + state);
        };
    }

    private static final int STARTING_COLUMN = 1;

    private static final int QUOTING_QUOTE = 2;

    private static final int QUOTING_COLUMN = 4;
}
