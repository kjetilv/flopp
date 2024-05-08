package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvEscapedLineSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder escFinder;

    private boolean escaping;

    private long lastEscape;

    BitwiseCsvEscapedLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.Escaped format) {
        this(lines, format, false);
    }

    BitwiseCsvEscapedLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.Escaped format, boolean immutable) {
        super(lines, format, immutable);
        this.escFinder = Bits.finder(format.escape(), format.fast());
    }

    @Override
    protected SeparatedLine process() {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
        this.startOffset = this.segment.startIndex();
        this.escaping = false;

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

    @Override
    protected String substring() {
        return formatString() + " " + (escaping ? "escaping" : "");
    }

    private void findSeps(long bytes, long shift) {
        int nextSep = sepFinder.next(bytes);
        int nextEsc = escFinder.next(bytes);

        while (true) {
            int diff = nextSep - nextEsc;
            if (diff == 0) {
                offset += ALIGNMENT;
                return;
            }
            if (diff < 0) {
                handleSep(nextSep, shift);
                nextSep = sepFinder.next();
            } else {
                handleEscape(nextEsc + shift);
                nextEsc = escFinder.next();
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

    private void handleSep(long index, long shift) {
        long adjusted = index + shift;
        long position = offset + adjusted;
        boolean effective = !(escaping && position == lastEscape + 1);
        if (effective) {
            markSeparator(position);
            currentStart = position;
        } else {
            escaping = false;
        }
    }

    private void handleEscape(long index) {
        if (escaping && offset + index == lastEscape + 1) {
            escaping = false;
        } else {
            lastEscape = offset + index;
            escaping = true;
        }
    }

}
