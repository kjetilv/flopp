package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("PackageVisibleField")
abstract class AbstractBitwiseCsvLineSplitter extends AbstractBitwiseLineSplitter implements LineSegment {

    final long[] startPositions;

    final long[] endPositions;

    final Bits.Finder sepFinder;

    long currentStart;

    long startOffset;

    long offset;

    int columnNo;

    LineSegment segment;

    private final CsvFormat format;

    private long startIndex;

    private long endIndex;

    AbstractBitwiseCsvLineSplitter(Consumer<SeparatedLine> lines, CsvFormat format, boolean immutable) {
        super(lines, immutable);
        this.format = Objects.requireNonNull(format, "format");

        this.sepFinder = Bits.finder(format.separator(), true);

        this.startPositions = new long[format.columnCount()];
        this.endPositions = new long[format.columnCount()];
    }

    @Override
    public final long startIndex() {
        return startIndex;
    }

    @Override
    public final long endIndex() {
        return endIndex;
    }

    @Override
    public MemorySegment memorySegment() {
        return segment.memorySegment();
    }

    @Override
    public int columnCount() {
        return columnNo;
    }

    @Override
    public final long[] start() {
        return startPositions;
    }

    @Override
    public final long[] end() {
        return endPositions;
    }

    @Override
    public long start(int column) {
        return startPositions[column];
    }

    @Override
    public long end(int column) {
        return endPositions[column];
    }

    @Override
    public final LineSegment segment(int column) {
        startIndex = startPositions[column];
        endIndex = endPositions[column];
        return this;
    }

    public long underlyingSize() {
        return segment.underlyingSize();
    }

    @Override
    public int hashCode() {
        return LineSegments.hashCode(this);
    }

    protected String formatString() {
        return "format=" + format.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LineSegment lineSegment && LineSegments.equals(this, lineSegment);
    }

    @Override
    protected LineSegment lineSegment() {
        return segment;
    }

    @Override
    protected String substring() {
        return formatString();
    }
}
