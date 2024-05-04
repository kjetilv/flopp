package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseEscapedCsvLineSplitter extends AbstractBitwiseCsvLineSplitter {

    private boolean quoting;

    private boolean escaping;

    private boolean quoted;

    private final Bits.Finder sepFinder;

    private final Bits.Finder quoFinder;

    private final Bits.Finder escFinder;

    BitwiseEscapedCsvLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.Escaped csvFormat) {
        this(lines, csvFormat, false);
    }

    BitwiseEscapedCsvLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.Escaped format, boolean immutable) {
        super(lines, format, immutable);
        Objects.requireNonNull(format, "lineSplit");

        this.sepFinder = Bits.finder(format.separator(), format.fast());
        this.quoFinder = Bits.finder(format.quote(), format.fast());
        this.escFinder = Bits.finder(format.escape(), format.fast());
    }

    @Override
    public void accept(LineSegment segment) {
        this.offset = this.currentStart = this.columnNo = 0;
        this.quoted = this.quoting = this.escaping = false;

        this.segment = segment;
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

    @Override
    protected String substring() {
        return formatString() + " " + (escaping ? "escaping"
            : quoting ? "quoting"
                : "");
    }

    private void findSeps(long bytes, long shift) {
        int nextSep = sepFinder.next(bytes);
        int nextQuo = quoFinder.next(bytes);
        int nextEsc = escFinder.next(bytes);

        while (true) {
            if (nextEsc == ALIGNMENT) { // Not escape
                if (nextSep < nextQuo) { // Separator
                    handleSep(nextSep + shift);
                    nextSep = sepFinder.next();
                } else if (nextSep == nextQuo) { // Nothing
                    handleNext();
                    return;
                } else { // Quote
                    handleQuo();
                    nextQuo = quoFinder.next();
                }
            } else {
                int min = Math.min(nextSep, Math.min(nextQuo, nextEsc));
                if (min == nextSep) { // Separator
                    handleSep(nextSep + shift);
                    nextSep = sepFinder.next();
                } else if (min == nextQuo) { // Quote
                    handleQuo();
                    nextQuo = quoFinder.next();
                } else { // Escape
                    handleEscape();
                    nextEsc = escFinder.next();
                }
            }
        }
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

    private void handleNext() {
        offset += ALIGNMENT;
    }

    private void handleSep(long index) {
        if (quoting) {
            return;
        }
        if (escaping) {
            escaping = false;
        } else {
            markColumn(index);
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

    private void handleEscape() {
        escaping = true;
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
}
