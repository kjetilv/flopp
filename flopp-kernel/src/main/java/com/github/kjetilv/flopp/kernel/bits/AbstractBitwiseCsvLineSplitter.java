package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("PackageVisibleField")
abstract sealed class AbstractBitwiseCsvLineSplitter extends AbstractBitwiseLineSplitter implements LineSegment
    permits BitwiseCsvDoubleQuotedLineSplitter, BitwiseCsvEscapedLineSplitter, BitwiseCsvSimpleLineSplitter {

    private final CsvFormat format;

    private long startIndex;

    private long endIndex;

    final long[] startPositions;

    final long[] endPositions;

    final Bits.Finder sepFinder;

    long currentStart;

    long startOffset;

    long offset;

    int columnNo;

    LineSegment segment;

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
    public final MemorySegment memorySegment() {
        return segment.memorySegment();
    }

    @Override
    public final int columnCount() {
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
    public final long start(int column) {
        return startPositions[column];
    }

    @Override
    public final long end(int column) {
        return endPositions[column];
    }

    @Override
    public final LineSegment segment(int column) {
        startIndex = startPositions[column];
        endIndex = endPositions[column];
        return this;
    }

    @Override
    public final int hashCode() {
        return LineSegments.hashCode(this);
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof LineSegment lineSegment && LineSegments.equals(this, lineSegment);
    }

    @Override
    final LineSegment lineSegment() {
        return segment;
    }

    @Override
    public final long underlyingSize() {
        return segment.underlyingSize();
    }

    @Override
    String substring() {
        return formatString();
    }

    final String formatString() {
        return "format=" + format.toString();
    }

    final void markSeparator(long length) {
        long startPosition = startOffset + currentStart + 1;
        long endPosition = startOffset + length;
        try {
            this.startPositions[columnNo] = startPosition;
            this.endPositions[columnNo] = endPosition;
            this.columnNo++;
        } catch (Exception e) {
            throw new IllegalStateException(
                this + " could not set column #" + (columnNo + 1) + ":" + startPosition + "-" + endPosition, e
            );
        }
    }
}
