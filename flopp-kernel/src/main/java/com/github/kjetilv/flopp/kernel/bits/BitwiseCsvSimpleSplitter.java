package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.segments.MemorySegments.ALIGNMENT_INT;

final class BitwiseCsvSimpleSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseCsvSimpleSplitter(Consumer<SeparatedLine> lines, CsvFormat format) {
        super(lines, format);
    }

    @Override
    protected void findSeps(long offset, long data, long endOffset) {
        int dist = sepFinder.next(data);
        long index;
        while (dist < ALIGNMENT_INT && (index = offset + dist) < endOffset) {
            markSeparator(index);
            dist = sepFinder.next();
        }
    }
}
