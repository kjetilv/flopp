package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;

final class BitwiseCsvSimpleSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseCsvSimpleSplitter(Consumer<SeparatedLine> lines, CsvFormat.Simple format) {
        super(lines, format);
    }

    @Override
    void separate(LineSegment segment) {
        int headStart = (int) this.startOffset % ALIGNMENT_INT;
        int headLen = 0;
        if (headStart != 0) {
            long data = segment.head(headStart);
            findSeps(offset, data, length);
            headLen = ALIGNMENT_INT - headStart;
            offset += headLen;
        }
        long endOffset = segment.endIndex();
        for (
            long i = this.startOffset + headLen;
            i < endOffset;
            i += ALIGNMENT_INT, offset += ALIGNMENT_INT
        ) {
            findSeps(offset, segment.longAt(i), length);
        }
        findSeps(offset, segment.tail(), length);
    }

    private void findSeps(long offset, long data, long length) {
        int dist = sepFinder.next(data);
        long index;
        while (dist < ALIGNMENT_INT && (index = offset + dist) < length) {
            markSeparator(index);
            dist = sepFinder.next();
        }
    }
}
