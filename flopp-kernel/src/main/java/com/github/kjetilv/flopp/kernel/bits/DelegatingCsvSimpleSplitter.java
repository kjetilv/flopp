package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;

@SuppressWarnings("DuplicatedCode")
final class DelegatingCsvSimpleSplitter implements LineSplitter, LineSegment, SeparatedLine {

    private final BitwiseTraverser.Reusable reusable;

    private final Consumer<SeparatedLine> lines;

    private final Bits.Finder sepFinder;

    private final long[] startPositions;

    private final long[] endPositions;

    private int columnNo;

    private MemorySegment memorySegment;

    DelegatingCsvSimpleSplitter(Consumer<SeparatedLine> lines, CsvFormat format) {
        this.lines = lines;
        this.sepFinder = Bits.swarFinder(format.separator());
        this.reusable = BitwiseTraverser.createAligned();
        this.startPositions = new long[format.columnCount()];
        this.endPositions = new long[format.columnCount()];
    }

    @Override
    public SeparatedLine apply(LineSegment lineSegment) {
        BitwiseTraverser.Reusable applied = reusable.apply(lineSegment);

        this.memorySegment = lineSegment.memorySegment();
        this.columnNo = 0;

        long startOffset = lineSegment.startIndex();
        long endOffset = lineSegment.endIndex();

        long currentColumnStart = startOffset;

        long count = lineSegment.alignedLongsCount();

        for (long offset = startOffset; offset < endOffset; offset += ALIGNMENT_INT) {
            long data = applied.getAsLong();
            int dist = sepFinder.next(data);
            while (dist < ALIGNMENT_INT) {
                long columnEnd = offset + dist;

                this.startPositions[columnNo] = currentColumnStart;
                this.endPositions[columnNo] = columnEnd;
                this.columnNo++;

                currentColumnStart = columnEnd + 1;
                dist = sepFinder.next();
            }
        }
        this.startPositions[columnNo] = currentColumnStart;
        this.endPositions[columnNo] = endOffset;
        this.columnNo++;
        lines.accept(this);
        return this;
    }

    @Override
    public MemorySegment memorySegment() {
        return memorySegment;
    }

    @Override
    public int columnCount() {
        return columnNo;
    }

    @Override
    public long[] start() {
        return startPositions;
    }

    @Override
    public long[] end() {
        return endPositions;
    }

    @Override
    public long startIndex() {
        return startPositions[columnNo];
    }

    @Override
    public long endIndex() {
        return endPositions[columnNo];
    }
}
