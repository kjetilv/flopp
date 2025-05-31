package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.Vectors;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

class VectorCsvSimpleLineSplitter
    implements SeparatedLine, LineSegment, Function<LineSegment, SeparatedLine>, Consumer<LineSegment> {

    private final Consumer<SeparatedLine> lines;

    private long startIndex;

    private long endIndex;

    private int columnNo;

    private final long[] startPositions;

    private final long[] endPositions;

    private MemorySegment memorySegment;

    private final Format.Csv format;

    VectorCsvSimpleLineSplitter(Format.Csv format, Consumer<SeparatedLine> lines) {
        this.lines = lines == null ? NONE : lines;
        this.format = Objects.requireNonNull(format, "format");
        this.startPositions = new long[this.format.columnCount()];
        this.endPositions = new long[this.format.columnCount()];
    }

    @Override
    public long startIndex() {
        return startIndex;
    }

    @Override
    public long endIndex() {
        return endIndex;
    }

    @Override
    public final MemorySegment memorySegment() {
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
    public void accept(LineSegment lineSegment) {
        apply(lineSegment);
    }

    @Override
    public final SeparatedLine apply(LineSegment lineSegment) {
        memorySegment = lineSegment.memorySegment();
        long start = lineSegment.startIndex();
        Vectors.Finder finder = Vectors.finder(memorySegment, start, format.separator());
        startPositions[0] = start;
        while (true) {
            long next = finder.next();
            if (next >= lineSegment.endIndex()) {
                endPositions[columnNo] = lineSegment.endIndex();
                columnNo = 0;
                lines.accept(this);
                return this;
            }
            endPositions[columnNo] = next;
            columnNo++;
            if (columnNo == format.columnCount()) {
                columnNo = 0;
                lines.accept(this);
                return this;
            }
            startPositions[columnNo] = next + 1;
        }
    }

    private static final Consumer<SeparatedLine> NONE = _ -> {
    };

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + format + "]";
    }
}
