package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvEscapeSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder escFinder;

    private boolean escaping;

    private long nextEscape;

    BitwiseCsvEscapeSplitter(Consumer<SeparatedLine> lines, CsvFormat.Escape format) {
        super(lines, format);
        this.escFinder = Bits.finder(format.escape(), format.fast());
    }

    @Override
    protected void separate() {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
        this.startOffset = this.segment.startIndex();
        this.escaping = false;

        long length = this.segment.length();
        if (length < MemorySegments.ALIGNMENT) {
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
                findSeps(this.segment.tail(), 0);
                markSeparator(length);
            }
        }
    }

    @Override
    protected String substring() {
        return formatString() + " " + (escaping ? "escaping" : "");
    }

    private void findSeps(long bytes, long shift) {
        int sep = sepFinder.next(bytes);
        int esc = escFinder.next(bytes);

        while (true) {
            int diff = sep - esc;
            if (diff == 0) {
                offset += MemorySegments.ALIGNMENT;
                return;
            }
            if (diff < 0) {
                handleSep(sep, shift);
                sep = sepFinder.next();
            } else {
                handleEsc(esc + shift);
                esc = escFinder.next();
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
        boolean effective = !(escaping && position == nextEscape);
        if (effective) {
            markSeparator(position);
            currentStart = position;
        } else {
            escaping = false;
        }
    }

    private void handleEsc(long index) {
        if (escaping && offset + index == nextEscape) {
            escaping = false;
        } else {
            nextEscape = offset + index + 1;
            escaping = true;
        }
    }
}
