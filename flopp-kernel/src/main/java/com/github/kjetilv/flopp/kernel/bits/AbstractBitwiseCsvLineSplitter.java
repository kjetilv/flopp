package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_POW;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

@SuppressWarnings("PackageVisibleField")
abstract sealed class AbstractBitwiseCsvLineSplitter
    extends AbstractBitwiseLineSplitter
    implements LineSegment
    permits BitwiseCsvQuotedSplitter, BitwiseCsvEscapeSplitter, BitwiseCsvSimpleSplitter {

    final Bits.Finder sepFinder;

    long currentStart;

    long startOffset;

    long offset;

    int columnNo;

    private final CsvFormat format;

    private final Charset charset;

    private final long[] startPositions;

    private final long[] endPositions;

    private final byte[] columnBuffer;

    private long startIndex;

    private long endIndex;

    AbstractBitwiseCsvLineSplitter(Consumer<SeparatedLine> lines, CsvFormat format) {
        super(lines);
        this.format = Objects.requireNonNull(format, "format");
        this.charset = Objects.requireNonNull(format.charset(), "format charset");
        this.sepFinder = Bits.finder(format.separator(), true);
        this.startPositions = new long[format.columnCount()];
        this.endPositions = new long[format.columnCount()];
        this.columnBuffer = new byte[format.maxColumnWidth()];
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
    public final Stream<String> columns(Charset charset) {
        return IntStream.range(0, columnCount())
            .mapToObj(i ->
                column(i, this.charset));
    }

    @Override
    public final String column(int column, Charset charset) {
        return MemorySegments.fromLongsWithinBounds(
            segment,
            startPositions[column],
            endPositions[column],
            columnBuffer,
            this.charset
        );
    }

    @Override
    public final LineSegment segment(int column) {
        startIndex = startPositions[column];
        endIndex = endPositions[column];
        return this;
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
    public final String asString(Charset charset) {
        return asString(null, charset);
    }

    @Override
    public final String asString(byte[] buffer, Charset charset) {
        return MemorySegments.fromLongsWithinBounds(
            segment,
            startIndex,
            endIndex,
            buffer,
            this.charset
        );
    }

    @Override
    public final long alignedStart() {
        return startIndex - startIndex % ALIGNMENT_INT;
    }

    @Override
    public final long alignedEnd() {
        return endIndex - endIndex % ALIGNMENT_INT;
    }

    @Override
    public final long alignedCount() {
        return alignedEnd() - alignedStart() >> ALIGNMENT_POW;
    }

    @Override
    public final long head() {
        long headOffset = startIndex % ALIGNMENT_INT;
        long offset = startIndex - headOffset;
        return segment.get(JAVA_LONG, offset) >>> headOffset * ALIGNMENT_INT;
    }

    @Override
    public long tail() {
        return MemorySegments.tail(segment, endIndex);
    }

    @Override
    public final int hashCode() {
        return LineSegments.hashCode(this);
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof LineSegment lineSegment && this.matches(lineSegment);
    }

    @Override
     final void init(LineSegment lineSegment) {
        this.segment = lineSegment.memorySegment();
        this.length = lineSegment.length();
        this.offset = 0;
        this.columnNo = 0;
        this.currentStart = -1;
        this.startOffset = lineSegment.startIndex();
    }

    @Override
    String substring() {
        return formatString();
    }

    final String formatString() {
        return "format=" + format.toString();
    }

    @Override
    protected void markEnd() {
        mark(length);
    }

    final void markSeparator(long index) {
        mark(index);
        currentStart = index;
    }

    private void mark(long length) {
        long startPosition = startOffset + currentStart + 1;
        long endPosition = startOffset + length;
        this.startPositions[columnNo] = startPosition;
        this.endPositions[columnNo] = endPosition;
        this.columnNo++;
    }
}
