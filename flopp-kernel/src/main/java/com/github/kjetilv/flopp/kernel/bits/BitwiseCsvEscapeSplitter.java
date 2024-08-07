package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;

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
    void separate(LineSegment segment) {
        this.escaping = false;
        long length = segment.length();
        if (length < MemorySegments.ALIGNMENT) {
            findSeps(segment.bytesAt(0, length), 0, 0);
        } else {
            long headStart = this.startOffset % ALIGNMENT_INT;
            long offset = processHead(segment, headStart);
            long start = this.startOffset - headStart + ALIGNMENT_INT;
            long end = segment.alignedEnd();
            for (long i = start; i < end; i += ALIGNMENT_INT) {
                findSeps(segment.longAt(i), 0, offset);
            }
            if (!segment.isAlignedAtEnd()) {
                findSeps(segment.tail(), 0, offset);
            }
        }
    }

    @Override
    protected String substring() {
        return formatString() + " " + (escaping ? "escaping" : "");
    }

    private void findSeps(long data, long shift, long offset) {
        int sep = sepFinder.next(data);
        int esc = escFinder.next(data);
        while (true) {
            int diff = sep - esc;
            if (diff == 0) {
                return;
            }
            if (diff < 0) {
                handleSep(sep, shift, offset);
                sep = sepFinder.next();
            } else {
                handleEsc(esc + shift, offset);
                esc = escFinder.next();
            }
        }
    }

    private long processHead(LineSegment segment, long headStart) {
        if (headStart == 0) {
            long headLong = segment.longNo(0);
            findSeps(headLong, 0, 0);
            return 0;
        } else {
            long headLong = segment.head(headStart);
            findSeps(headLong, headStart, -headStart);
            return -headStart;
        }
    }

    private void handleSep(long index, long shift, long offset) {
        long adjusted = index + shift;
        long position = offset + adjusted;
        boolean effective = !(escaping && position == nextEscape);
        if (effective) {
            markSeparator(position);
        } else {
            escaping = false;
        }
    }

    private void handleEsc(long index, long offset) {
        if (escaping && offset + index == nextEscape) {
            escaping = false;
        } else {
            nextEscape = offset + index + 1;
            escaping = true;
        }
    }
}
