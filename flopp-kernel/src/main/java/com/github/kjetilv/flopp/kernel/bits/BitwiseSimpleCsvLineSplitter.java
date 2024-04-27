package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * @noinspection DuplicatedCode
 */
final class BitwiseSimpleCsvLineSplitter extends AbstractBitwiseLineSplitter implements LineSegment {

    private final long[] start;

    private final long[] end;

    private LineSegment segment;

    private long currentStart;

    private long startOffset;

    private long offset;

    private int columnNo;

    private final Bits.Finder sepFinder;

    private long lineSegmentStart;

    private long lineSegmentEnd;

    BitwiseSimpleCsvLineSplitter(
        Consumer<SeparatedLine> lines,
        CsvFormat.Simple format,
        boolean immutable
    ) {
        super(lines, immutable);
        Objects.requireNonNull(format, "lineSplit");

        this.sepFinder = Bits.finder(format.separator(), true);

        this.start = new long[format.columnCount()];
        this.end = new long[format.columnCount()];
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
    public long[] start() {
        return start;
    }

    @Override
    public long[] end() {
        return end;
    }

    @Override
    public LineSegment segment(int column) {
        lineSegmentStart = start[column];
        lineSegmentEnd = end[column];
        return this;
    }

    @Override
    public long startIndex() {
        return lineSegmentStart;
    }

    @Override
    public long endIndex() {
        return lineSegmentEnd;
    }

    @Override
    public void accept(LineSegment segment) {
        this.offset = this.currentStart = this.columnNo = 0;

        this.segment = Objects.requireNonNull(segment, "segment");
        this.startOffset = this.segment.startIndex();

        long length = this.segment.length();
        if (length < ALIGNMENT) {
            findSeps(this.segment.bytesAt(0, length), 0);
            addSep(length);
        } else {
            processHead();
            long longCount = this.segment.alignedCount();
            for (int i = 1; i < longCount; i++) {
                findSeps(this.segment.longNo(i), 0);
            }
            if (this.segment.isAlignedAtEnd()) {
                addSep(length);
            } else {
                findSeps(this.segment.tail(), 0);
                addSep(length);
            }
        }
        emit();
    }

    @Override
    protected LineSegment lineSegment() {
        return segment;
    }

    @Override
    protected String substring() {
        return "";
    }

    private void processHead() {
        long headStart = this.segment.headStart();
        if (headStart == 0) {
            long headLong = this.segment.longNo(0);
            findSeps(headLong, 0);
        } else {
            offset = -headStart;
            long headLong = this.segment.head(headStart);
            findSeps(headLong, headStart);
        }
    }

    private void findSeps(long bytes, long shift) {
        int nextSep = sepFinder.next(bytes);
        while (true) {
            if (nextSep == ALIGNMENT) { // No match
                offset += ALIGNMENT;
                return;
            }
            handleSep(nextSep + shift);
            nextSep = sepFinder.next();
        }
    }

    private void handleSep(long index) {
        markColumn(index);
    }

    private void markColumn(long index) {
        long end = offset + index;
        addSep(end);
        currentStart = end + 1;
    }

    private void addSep(long end) {
        try {
            this.start[columnNo] = startOffset + currentStart;
            this.end[columnNo] = startOffset + end;
            this.columnNo++;
        } catch (Exception e) {
            throw new IllegalStateException(this + " could not set " + columnNo, e);
        }
    }
}
