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

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

@SuppressWarnings("PackageVisibleField")
abstract sealed class AbstractBitwiseCsvLineSplitter extends AbstractBitwiseLineSplitter implements LineSegment
    permits BitwiseCsvDoubleQuotedLineSplitter, BitwiseCsvEscapedLineSplitter, BitwiseCsvSimpleLineSplitter {

    private final CsvFormat format;

    private final Charset charset;

    private final long[] startPositions;

    private final long[] endPositions;

    private final byte[] columnBuffer;

    final Bits.Finder sepFinder;

    long currentStart;

    long startOffset;

    long offset;

    int columnNo;

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
    public String column(int column, Charset charset) {
        return MemorySegments.fromLongsWithinBounds(
            memorySegment,
            startPositions[column],
            endPositions[column],
            columnBuffer,
            this.charset
        );
    }

    @Override
    public Stream<String> columns(Charset charset) {
        return IntStream.range(0, columnCount()).mapToObj(i ->
            column(i, this.charset));
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
    public final long startIndex() {
        return startIndex;
    }

    @Override
    public final long endIndex() {
        return endIndex;
    }

    @Override
    public String asString(Charset charset) {
        return asString(null, charset);
    }

    @Override
    public String asString(byte[] buffer, Charset charset) {
        return MemorySegments.fromLongsWithinBounds(
            memorySegment,
            startIndex,
            endIndex,
            buffer,
            this.charset
        );
    }

    @Override
    public long headStart() {
        return startIndex % ALIGNMENT;
    }

    @Override
    public boolean isAlignedAtStart() {
        return startIndex % ALIGNMENT == 0L;
    }

    @Override
    public boolean isAlignedAtEnd() {
        return endIndex % ALIGNMENT == 0;
    }

    @Override
    public long head(boolean truncate) {
        long readLength = MemorySegments.readLength(startIndex, endIndex);
        if (this.underlyingSize() - startIndex < ALIGNMENT) {
            return MemorySegments.readHead(memorySegment, startIndex, readLength);
        }
        long value = memorySegment.get(JAVA_LONG_UNALIGNED, startIndex);
        return truncate
            ? Bits.lowerBytes(value, Math.toIntExact(readLength))
            : value;
    }

    @Override
    public long head(long head) {
        long l = memorySegment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT);
        return l >> head * ALIGNMENT;
    }

    @Override
    public long longNo(int longNo) {
        return memorySegment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT + longNo * ALIGNMENT);
    }

    @Override
    public long tail(boolean truncate) {
        int tail = Math.toIntExact(endIndex % ALIGNMENT);
        if (underlyingSize - endIndex < ALIGNMENT) {
            return MemorySegments.readTail(memorySegment, endIndex, tail);
        }
        long value = memorySegment.get(JAVA_LONG_UNALIGNED, endIndex - endIndex % ALIGNMENT);
        return truncate
            ? Bits.lowerBytes(value, tail)
            : value;
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
