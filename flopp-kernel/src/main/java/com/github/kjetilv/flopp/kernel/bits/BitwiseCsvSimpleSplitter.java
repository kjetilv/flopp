package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

final class BitwiseCsvSimpleSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseCsvSimpleSplitter(
        Consumer<SeparatedLine> lines,
        CsvFormat.Simple format
    ) {
        super(
            lines,
            format
        );
    }

    @Override
    void separate(LineSegment segment) {
        long headStart = this.startOffset % ALIGNMENT_INT;
        long headLong = segment.longAt(startOffset - headStart);
        findSeps(startOffset, headLong >>> headStart * ALIGNMENT_INT);
        long offset = startOffset + ALIGNMENT_INT - headStart;
        while (offset < endOffset) {
            findSeps(offset, segment.longAt(offset));
            offset += ALIGNMENT_INT;
        }
    }

    private void findSeps(
        long offset,
        long data
    ) {
        int dist = sepFinder.next(data);
        long index;
        while (dist < ALIGNMENT_INT && (index = offset + dist) < endOffset) {
            markSeparator(index);
            dist = sepFinder.next();
        }
    }
}
