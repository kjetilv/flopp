package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseDoubleQuotedCsvLineSplitter extends AbstractBitwiseCsvLineSplitter {

    private State state;

    private boolean quoted;

    private final Bits.Finder quoFinder;

    BitwiseDoubleQuotedCsvLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.DoubleQuoted csvFormat) {
        this(lines, csvFormat, false);
    }

    BitwiseDoubleQuotedCsvLineSplitter(
        Consumer<SeparatedLine> lines,
        CsvFormat.DoubleQuoted format,
        boolean immutable
    ) {
        super(lines, format, immutable);
        Objects.requireNonNull(format, "lineSplit");

        this.quoFinder = Bits.finder(format.quote(), format.fast());

        this.state = State.STARTING_COLUMN;
    }

    @Override
    public void accept(LineSegment segment) {
        this.offset = this.currentStart = this.columnNo = 0;
        this.quoted = false;
        this.state = State.STARTING_COLUMN;

        this.segment = Objects.requireNonNull(segment, "segment");
        this.startOffset = this.segment.startIndex();

        long length = this.segment.length();
        if (length < ALIGNMENT) {
            findSeps(this.segment.bytesAt(0, length), 0);
            addSep(length);
        } else {
            processHead();
            long longCount = this.segment.alignedCount();
            for (int i = 1; i < longCount; i++) {
                findSeps(this.segment.longNo(i), 0);
            }
            if (this.segment.isAlignedAtEnd()) {
                addSep(length);
            } else {
                findSeps(this.segment.tail(true), 0);
                addSep(length);
            }
        }
        emit();
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
            if (nextSep == nextQuo) { // No match
                offset += ALIGNMENT;
                return;
            }
            if (nextSep < nextQuo) { // Sep
                handleSep(nextSep + shift);
                nextSep = sepFinder.next();
            } else { // Quo
                handleQuo();
                nextQuo = quoFinder.next();
            }
        }
    }

    private void handleSep(long index) {
        if (state == State.STARTING_COLUMN) {
            markColumn(index);
        } else if (state == State.QUOTING_QUOTE) {
            markColumn(index);
            state = State.STARTING_COLUMN;
        }
    }

    private void handleQuo() {
        if (state == State.STARTING_COLUMN) {
            state = State.QUOTING_COLUMN;
            quoted = true;
        } else if (state == State.QUOTING_COLUMN) {
            state = State.QUOTING_QUOTE;
        } else if (state == State.QUOTING_QUOTE) {
            state = State.QUOTING_COLUMN;
        }
    }

    private void markColumn(long index) {
        long end = offset + index;
        addSep(end);
        currentStart = end + 1;
    }

    private void addSep(long end) {
        int quote = quoted ? 1 : 0;
        this.startPositions[columnNo] = startOffset + currentStart + quote;
        this.endPositions[columnNo] = startOffset + end - quote;
        this.columnNo++;
        this.quoted = false;
    }

    private enum State {

        STARTING_COLUMN,

        QUOTING_COLUMN,

        QUOTING_QUOTE
    }
}
