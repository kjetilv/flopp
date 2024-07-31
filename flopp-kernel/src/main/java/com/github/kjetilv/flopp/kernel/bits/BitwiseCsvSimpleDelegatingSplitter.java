package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvSimpleDelegatingSplitter extends AbstractBitwiseCsvLineSplitter {

    private final BitwiseTraverser.Reusable reusable;

    BitwiseCsvSimpleDelegatingSplitter(Consumer<SeparatedLine> lines, CsvFormat.Simple format) {
        super(lines, format);
        this.reusable = BitwiseTraverser.create(true);
    }

    @Override
    void separate(LineSegment segment) {
        BitwiseTraverser.Reusable applied = reusable.apply(segment);
        long l = segment.alignedLongsCount();
        for (long i = 0; i < l; i++) {
            long data = applied.getAsLong();
            findSeps(offset, data);
        }
        long endOffset = segment.endIndex();
        int headStart = (int) this.startOffset % ALIGNMENT_INT;
        long start = this.startOffset;
        if (headStart != 0) {
            long data = applied.getAsLong();
            findSeps(offset, data);
            int headLen = ALIGNMENT_INT - headStart;
            offset += headLen;
            start += headLen;
        }
        long end = endOffset - endOffset % ALIGNMENT_INT;
        for (long i = start; i < end; i += ALIGNMENT_INT, offset += ALIGNMENT_INT) {
            findSeps(offset, applied.getAsLong());
        }
        findSeps(offset, applied.getAsLong());
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
