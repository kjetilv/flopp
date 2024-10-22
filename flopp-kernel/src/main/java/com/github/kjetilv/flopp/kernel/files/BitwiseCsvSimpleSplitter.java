package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.segments.MemorySegments.ALIGNMENT_INT;

final class BitwiseCsvSimpleSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseCsvSimpleSplitter(Consumer<SeparatedLine> lines, Format.Csv format) {
        super(lines, format);
    }

    @Override
    protected String substring() {
        return formatString();
    }

    @Override
    protected void findSeps(long offset, long data, long endOffset) {
        int dist = nextSep(data);
        long index;
        while (dist < ALIGNMENT_INT && (index = offset + dist) < endOffset) {
            markSeparator(index);
            dist = nextSep();
        }
    }
}
