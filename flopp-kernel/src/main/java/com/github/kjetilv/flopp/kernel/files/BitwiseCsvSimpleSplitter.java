package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_INT;

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
        int range = (int) (endOffset - offset);
        int dist = nextSep(data);
        while (dist < ALIGNMENT_INT && dist < range) {
            markSeparator(offset + dist);
            dist = nextSep();
        }
    }
}
