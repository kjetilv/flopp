package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvSimpleSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseCsvSimpleSplitter(Consumer<SeparatedLine> lines, CsvFormat.Simple format) {
        super(lines, format);
    }

    @Override
    protected void separate(LineSegment segment) {
        this.offset = this.columnNo = 0;
        this.currentStart = -1;
        this.startOffset = segment.startIndex();

        long length = segment.length();
        if (length < ALIGNMENT_INT) {
            long data = segment.bytesAt(0, length);
            findSeps(data);
            markSeparator(length);
        } else {
            long headStart = this.startOffset % ALIGNMENT_INT;
            findInitialSeps(segment.head(headStart), headStart);
            long start = this.startOffset - headStart + ALIGNMENT_INT;
            long end = segment.alignedEnd();
            for (long i = start; i < end; i += ALIGNMENT_INT) {
                findSeps(segment.longAt(i));
            }
            if (segment.isAlignedAtEnd()) {
                markSeparator(length);
            } else {
                findSeps(segment.tail());
                markSeparator(length);
            }
        }
    }

    private void findInitialSeps(long data, long headStart) {
        int dist = sepFinder.next(data);
        while (dist < ALIGNMENT_INT) {
            markSeparator(dist);
            currentStart = dist;
            dist = sepFinder.next();
        }
        offset += ALIGNMENT_INT - headStart;
    }

    private void findSeps(long data) {
        int dist = sepFinder.next(data);
        while (dist < ALIGNMENT_INT) {
            long index = offset + dist;
            markSeparator(index);
            currentStart = index;
            dist = sepFinder.next();
        }
        offset += ALIGNMENT_INT;
    }
}
