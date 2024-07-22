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
    void separate(LineSegment segment) {
        long endOffset = segment.endIndex();
        int headStart = (int) this.startOffset % ALIGNMENT_INT;
        long start = this.startOffset;
        if (headStart != 0) {
            findSeps(offset, segment.head(headStart));
            int headLen = ALIGNMENT_INT - headStart;
            offset += headLen;
            start += headLen;
        }
        long end = endOffset - endOffset % ALIGNMENT_INT;
        for (long i = start; i < end; i += ALIGNMENT_INT, offset += ALIGNMENT_INT) {
            findSeps(offset, segment.longAt(i));
        }
        findSeps(offset, segment.tail());
    }

    private void findSeps(long offset, long data) {
        int dist = sepFinder.next(data);
        while (dist < ALIGNMENT_INT) {
            long index = offset + dist;
            markSeparator(index);
            dist = sepFinder.next();
        }
    }
}
