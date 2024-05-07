package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvDoubleQuotedLineSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder quoFinder;

    private State state;

    BitwiseCsvDoubleQuotedLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.DoubleQuoted format) {
        this(lines, format, false);
    }

    BitwiseCsvDoubleQuotedLineSplitter(
        Consumer<SeparatedLine> lines,
        CsvFormat.DoubleQuoted format,
        boolean immutable
    ) {
        super(lines, format, immutable);
        this.quoFinder = Bits.finder(format.quote(), format.fast());
    }

    @Override
    public SeparatedLine apply(LineSegment segment) {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
        this.state = State.STARTING_COLUMN;

        this.segment = Objects.requireNonNull(segment, "segment");
        this.startOffset = this.segment.startIndex();

        long length = this.segment.length();
        if (length < ALIGNMENT) {
            findSeps(this.segment.bytesAt(0, length), 0);
            markSeparator(length);
        } else {
            processHead();
            long longCount = this.segment.alignedCount();
            for (int i = 1; i < longCount; i++) {
                findSeps(this.segment.longNo(i), 0);
            }
            if (this.segment.isAlignedAtEnd()) {
                markSeparator(length);
            } else {
                findSeps(this.segment.tail(true), 0);
                markSeparator(length);
            }
        }
        return emit(sl());
    }

    private void processHead() {
        long headStart = this.segment.headStart();
        if (headStart == 0) {
            long headLong = this.segment.longNo(0);
            findSeps(headLong, 0);
        } else {
            offset = -headStart;
            long headLong = this.segment.head(headStart);
            findSeps(headLong, headStart);
        }
    }

    private void findSeps(long bytes, long shift) {
        int nextSep = sepFinder.next(bytes);
        int nextQuo = quoFinder.next(bytes);

        while (true) {
            int diff = nextSep - nextQuo;
            if (diff == 0) {
                offset += ALIGNMENT;
                return;
            }
            if (diff < 0) {
                handleSep(nextSep + shift);
                nextSep = sepFinder.next();
            } else {
                handleQuo();
                nextQuo = quoFinder.next();
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
                state = State.STARTING_COLUMN;
            }
        }
    }

    private void handleQuo() {
        state = switch (state) {
            case STARTING_COLUMN, QUOTING_QUOTE -> State.QUOTING_COLUMN;
            case QUOTING_COLUMN -> State.QUOTING_QUOTE;
        };
    }

    private enum State {

        STARTING_COLUMN,

        QUOTING_QUOTE,

        QUOTING_COLUMN
    }
}
