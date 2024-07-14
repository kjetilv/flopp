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
            long bytes = segment.bytesAt(0, length);
            findSeps(bytes, 0);
            markSeparator(length);
        } else {
            processHead(segment);
            long start = segment.alignedStart() + ALIGNMENT_INT;
            long end = segment.alignedEnd();
            for (long i = start; i < end; i += ALIGNMENT_INT) {
                findSeps(segment.longAt(i), 0);
            }
            if (segment.isAlignedAtEnd()) {
                markSeparator(length);
            } else {
                findSeps(segment.tail(), 0);
                markSeparator(length);
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

    private void findSeps(long bytes, long shift) {
        int dist = sepFinder.next(bytes);
        while (dist < ALIGNMENT_INT) {
            long index = offset + dist + shift;
            markSeparator(index);
            currentStart = index;
            dist = sepFinder.next();
        }
        offset += ALIGNMENT_INT;
    }
}
