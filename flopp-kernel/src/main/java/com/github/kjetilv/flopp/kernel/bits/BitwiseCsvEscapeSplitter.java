package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
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
    protected void separate(LineSegment segment) {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
        this.startOffset = segment.startIndex();
        this.escaping = false;

        long length = segment.length();
        if (length < MemorySegments.ALIGNMENT) {
            findSeps(segment.bytesAt(0, length), 0);
            markSeparator(length);
        } else {
            processHead(segment);
            long longCount = segment.alignedCount();
            for (int i = 1; i < longCount; i++) {
                findSeps(segment.longNo(i), 0);
            }
            if (segment.isAlignedAtEnd()) {
                markSeparator(length);
            } else {
                findSeps(segment.tail(), 0);
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

    private void processHead(LineSegment segment) {
        long headStart = segment.headStart();
        if (headStart == 0) {
            long headLong = segment.longNo(0);
            findSeps(headLong, 0);
        } else {
            offset = -headStart;
            long headLong = segment.head(headStart);
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
