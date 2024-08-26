package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.Range;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;

import java.nio.charset.Charset;
import java.util.function.Consumer;

final class BitwiseFwLineSplitter extends AbstractBitwiseLineSplitter {

    private final long[] start;

    private final long[] end;

    private final int length;

    private int column;

    BitwiseFwLineSplitter(FwFormat format, Consumer<SeparatedLine> lines) {
        super(lines);
        Range[] ranges = format.ranges();
        this.length = ranges.length;
        this.start = new long[this.length];
        this.end = new long[this.length];
        for (int i = 0; i < this.length; i++) {
            Range range = ranges[i];
            this.start[i] = range.startIndex();
            this.end[i] = range.endIndex();
        }
    }

    @Override
    public int columnCount() {
        return length;
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
    public String column(int column, Charset charset) {
        return "";
    }

    @Override
    public long start(int column) {
        return start[column];
    }

    @Override
    public long end(int column) {
        return end[column];
    }

    @Override
    public LineSegment segment(int column) {
        this.column = column;
        return this;
    }

    @Override
    public long startIndex() {
        return start[column];
    }

    @Override
    public long endIndex() {
        return end[column];
    }

    @Override
    void init(LineSegment lineSegment) {
        this.segment = lineSegment.memorySegment();
    }
}
