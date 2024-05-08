package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvSimpleLineSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseCsvSimpleLineSplitter(Consumer<SeparatedLine> lines, CsvFormat.Simple format, boolean immutable) {
        super(lines, format, immutable);
    }

    @Override
    protected SeparatedLine process() {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
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
        while (true) {
            if (nextSep == ALIGNMENT) {
                offset += ALIGNMENT;
                return;
            }
            long index = offset + nextSep + shift;
            markSeparator(index);
            currentStart = index;
            nextSep = sepFinder.next();
        }
    }
}
